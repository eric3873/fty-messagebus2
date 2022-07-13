/*  =========================================================================
    Copyright (C) 2014 - 2021 Eaton

    This program is free software; you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation; either version 2 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License along
    with this program; if not, write to the Free Software Foundation, Inc.,
    51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA.
    =========================================================================
*/

#include "MsgBusMqtt.h"
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty/messagebus2/mqtt/MessageBusMqtt.h>
#include <fty/messagebus2/utils.h>
#include <fty_log.h>
#include <mqtt/async_client.h>
#include <mqtt/client.h>
#include <mqtt/message.h>
#include <mqtt/properties.h>

namespace fty::messagebus2::mqtt {

using namespace fty::messagebus2;
using duration = int64_t;

duration KEEP_ALIVE           = 20;
static auto constexpr _QOS    = ::mqtt::ReasonCode::GRANTED_QOS_2;
auto constexpr TIMEOUT        = std::chrono::seconds(5);
auto constexpr DOUBLE_TIMEOUT = std::chrono::seconds(10);

static const MetaData getMetaDataFromMqttProperties(const ::mqtt::properties& props)
{
    Message message;

    // User properties
    if (props.contains(::mqtt::property::USER_PROPERTY)) {
        std::string key, value;
        for (size_t i = 0; i < props.count(::mqtt::property::USER_PROPERTY); ++i) {
            std::tie(key, value) = ::mqtt::get<::mqtt::string_pair>(props, ::mqtt::property::USER_PROPERTY, i);
            message.setMetaDataValue(key, value);
        }
    }
    // Req/Rep pattern properties
    if (props.contains(::mqtt::property::CORRELATION_DATA)) {
        message.correlationId(::mqtt::get<std::string>(props, ::mqtt::property::CORRELATION_DATA));
    }

    if (props.contains(::mqtt::property::RESPONSE_TOPIC)) {
        message.replyTo(::mqtt::get<std::string>(props, ::mqtt::property::RESPONSE_TOPIC));
    }
    return message.metaData();
}

static const ::mqtt::properties getMqttProperties(const Message& message)
{
    auto props = ::mqtt::properties{};
    for (const auto& [key, value] : message.metaData()) {
        if (key == REPLY_TO) {
            props.add({::mqtt::property::CORRELATION_DATA, message.correlationId()});
            props.add({::mqtt::property::RESPONSE_TOPIC, message.replyTo()});
        } else if (key != CORRELATION_ID) {
            props.add({::mqtt::property::USER_PROPERTY, key, value});
        }
    }
    return props;
}

static ::mqtt::message_ptr buildMessageForMqtt(const Message& message)
{
    // Adding all meta data inside mqtt properties
    auto props = getMqttProperties(message);

    // get retain
    bool retain = (message.getMetaDataValue(mqtt::RETAIN) == "true");

    // get QoS
    ::mqtt::ReasonCode QoS = ::mqtt::ReasonCode::GRANTED_QOS_2;
    if (message.getMetaDataValue(mqtt::QOS) == "1") {
        QoS = ::mqtt::ReasonCode::GRANTED_QOS_1;
    } else if (message.getMetaDataValue(mqtt::QOS) == "0") {
        QoS = ::mqtt::ReasonCode::GRANTED_QOS_0;
    }

    auto msgToSend = ::mqtt::message_ptr_builder()
                         .topic(message.to())
                         .payload(message.userData())
                         .qos(QoS)
                         .properties(props)
                         .retained(retain)
                         .finalize();
    return msgToSend;
}

MsgBusMqtt::~MsgBusMqtt()
{
    // Cleaning all async/sync mqtt clients
    if (m_asynClient) {
        logDebug("Asynchronous client cleaning ...");
        m_asynClient->disable_callbacks();
        m_asynClient->stop_consuming();
        if (m_asynClient->is_connected()) {
            m_asynClient->disconnect()->wait();
        }
        logDebug("Asynchronous client cleaned");
    }
}

fty::Expected<void, ComState> MsgBusMqtt::connect()
{
    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    ::mqtt::create_options opts(MQTTVERSION_5);

    m_asynClient = std::make_shared<::mqtt::async_client>(m_endpoint, utils::getClientId("async-" + m_clientName), opts);

    // Connection options
    ::mqtt::connect_options connOpts = ::mqtt::connect_options_builder()
                                           .clean_session(false)
                                           .mqtt_version(MQTTVERSION_5)
                                           .keep_alive_interval(std::chrono::seconds(KEEP_ALIVE))
                                           .connect_timeout(TIMEOUT)
                                           .automatic_reconnect(TIMEOUT, DOUBLE_TIMEOUT)
                                           .clean_start(true)
                                           .finalize();

    if (m_will.isValidMessage()) {
        ::mqtt::will_options willOptions(*buildMessageForMqtt(m_will));
        connOpts.set_will(willOptions);
    }

    try {
        // Start consuming _before_ connecting, because we could get a flood
        // of stored messages as soon as the connection completes since
        // we're using a persistent (non-clean) session with the broker.
        m_asynClient->start_consuming();
        m_asynClient->connect(connOpts)->wait();

        // Called after a reconnection
        m_asynClient->set_connected_handler([this](const std::string& cause) {
            (cause.empty()) ? logDebug("Connected") : logDebug("{}", cause);
            // Refresh all recieved
            for (auto [address, listener] : m_cb.subscriptions()) {
                auto received = receive(address, listener);
                if (!received) {
                    logWarn("Address '{}' rejected after reconnection", address);
                }
            }
        });

        // After a connection lost, depending of reconnection setting, this handler is called by paho library
        m_asynClient->set_update_connection_handler([this](const ::mqtt::connect_data& /*connData*/) {
            if (!m_asynClient->is_connected()) {
                logInfo("Try a reconnection with {} ...", m_endpoint);
                m_asynClient->reconnect()->wait_for(TIMEOUT);
            }
            return true;
        });

        // Callback(s)
        m_asynClient->set_callback(m_cb);
        logInfo("{} => connect status: async client: {}", m_clientName, m_asynClient->is_connected() ? "true" : "false");
    } catch (const ::mqtt::exception& e) {
        logError("Error to connect with the Mqtt server, reason: {}", e.get_message());
        return fty::unexpected(ComState::ConnectFailed);
    } catch (const std::exception& e) {
        logError("unexpected error: {}", e.what());
        return fty::unexpected(ComState::ConnectFailed);
    }

    return {};
}

fty::Expected<void, DeliveryState> MsgBusMqtt::receive(const Address& address, MessageListener messageListener)
{
    if (!isServiceAvailable()) {
        logError("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    if (!m_cb.subscribed(address)) {
        m_cb.subscriptions(address, messageListener);
        m_asynClient->set_message_callback([this](::mqtt::const_message_ptr msg) {
            // Wrapper from mqtt msg to Message
            m_cb.onMessageArrived(msg);
        });

        if (!m_asynClient->subscribe(address, _QOS)->wait_for(TIMEOUT)) {
            logError("Receive for {} (Rejected)", address);
            return fty::unexpected(DeliveryState::Rejected);
        }
    } else {
        // Here after a reconnection
        m_asynClient->subscribe(address, _QOS)->wait_for(TIMEOUT);
    }

    logDebug("Waiting to receive msg from: {} Accepted", address);
    return {};
}

fty::Expected<void, DeliveryState> MsgBusMqtt::unreceive(const Address& address)
{
    if (!isServiceAvailable()) {
        logError("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    if (!m_cb.subscribed(address)) {
        logError("Address not found {}, unsubscribed (Rejected)", address);
        return fty::unexpected(DeliveryState::Rejected);
    }

    m_asynClient->unsubscribe(address)->wait_for(TIMEOUT);
    m_cb.eraseSubscriptions(address);
    logDebug("Unreceive for {} Accepted", address);
    return {};
}

fty::Expected<void, DeliveryState> MsgBusMqtt::send(const Message& message)
{
    if (!isServiceAvailable()) {
        logError("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    logTrace("Sending message {}", message.toString());

    auto msgToSend = buildMessageForMqtt(message);

    if (!m_asynClient->publish(msgToSend)->wait_for(TIMEOUT)) {
        logError("Message sent (Rejected)");
        return fty::unexpected(DeliveryState::Rejected);
    }

    logTrace("Message sent (Accepted)");
    return {};
}

fty::Expected<Message, DeliveryState> MsgBusMqtt::request(const Message& message, int receiveTimeOut)
{
    if (!isServiceAvailable()) {
        logError("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    ::mqtt::const_message_ptr msg;
    m_asynClient->subscribe(message.replyTo(), _QOS);
    auto msgSent = send(message);
    if (!msgSent) {
        return fty::unexpected(DeliveryState::Rejected);
    }

    auto messageArrived = m_asynClient->try_consume_message_for(&msg, std::chrono::seconds(receiveTimeOut));
    m_asynClient->unsubscribe(message.replyTo());
    if (!messageArrived) {
        logError("No message arrive in time!");
        return fty::unexpected(DeliveryState::Timeout);
    }

    logDebug("Message arrived ({})", msg->get_payload_str().c_str());
    return Message(getMetaDataFromMqttProperties(msg->get_properties()), msg->get_payload_str());
}

bool MsgBusMqtt::isServiceAvailable()
{
    return m_asynClient && m_asynClient->is_connected();
}

} // namespace fty::messagebus2::mqtt
