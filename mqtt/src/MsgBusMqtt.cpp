/*  =========================================================================
    MsgBusMqtt.cpp - class description

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

/*
@header
    MsgBusMqtt -
@discuss
@end
*/

#include "MsgBusMqtt.h"
#include "MsgBusMqttUtils.h"

#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/utils.h>
#include <fty_log.h>

#include <mqtt/async_client.h>
#include <mqtt/client.h>
#include <mqtt/message.h>
#include <mqtt/properties.h>

namespace
{
  template <typename ClientType>
  bool isServiceAvailable(std::shared_ptr<ClientType> client)
  {
    bool serviceAvailable = client && client->is_connected();
    return serviceAvailable;
  }
} // namespace

namespace fty::messagebus::mqtt
{
  using namespace fty::messagebus;

  using duration = int64_t;

  duration KEEP_ALIVE = 20;
  static auto constexpr QOS = ::mqtt::ReasonCode::GRANTED_QOS_2;
  static auto constexpr RETAINED = false; //true;
  auto constexpr TIMEOUT = std::chrono::seconds(5);

  MsgBusMqtt::~MsgBusMqtt()
  {
    // Cleaning all async/sync mqtt clients
    if (isServiceAvailable(m_asynClient))
    {
      logDebug("Asynchronous client cleaning");
      sendServiceStatus(DISCONNECTED_MSG);
      m_asynClient->disable_callbacks();
      m_asynClient->stop_consuming();
      m_asynClient->disconnect()->wait();
    }
    if (isServiceAvailable(m_synClient))
    {
      logDebug("Synchronous client cleaning");
      m_synClient->stop_consuming();
      m_synClient->disconnect();
    }
  }

  fty::Expected<void> MsgBusMqtt::connect()
  {
    logDebug("Connecting to {} ...", m_endpoint);
    ::mqtt::create_options opts(MQTTVERSION_5);

    m_asynClient = std::make_shared<::mqtt::async_client>(m_endpoint, utils::getClientId("async-" + m_clientName), opts);
    m_synClient = std::make_shared<::mqtt::client>(m_endpoint, utils::getClientId("sync-" + m_clientName), opts);

    // Connection options
    auto connOpts = ::mqtt::connect_options_builder()
                      .clean_session(false)
                      .mqtt_version(MQTTVERSION_5)
                      .keep_alive_interval(std::chrono::seconds(KEEP_ALIVE))
                      .automatic_reconnect(true)
                      //.automatic_reconnect(std::chrono::seconds(1), std::chrono::seconds(5))
                      .clean_start(true)
                      .will(::mqtt::message{DISCOVERY_TOPIC + m_clientName + DISCOVERY_TOPIC_SUBJECT, {DISAPPEARED_MSG}, QOS, true})
                      .finalize();
    try
    {
      // Start consuming _before_ connecting, because we could get a flood
      // of stored messages as soon as the connection completes since
      // we're using a persistent (non-clean) session with the broker.
      m_asynClient->start_consuming();
      m_asynClient->connect(connOpts)->wait();

      m_synClient->start_consuming();
      m_synClient->connect(connOpts);

      // Callback(s)
      m_asynClient->set_callback(m_cb);
      m_synClient->set_callback(m_cb);
      m_asynClient->set_connected_handler([this](const std::string& cause) {
        m_cb.onConnected(cause);
      });

      m_asynClient->set_update_connection_handler([this](const ::mqtt::connect_data& connData) {
        return m_cb.onConnectionUpdated(connData);
      });
      m_synClient->set_update_connection_handler([this](const ::mqtt::connect_data& connData) {
        return m_cb.onConnectionUpdated(connData);
      });

      logInfo("{} => connect status: sync client: {}, async client: {}", m_clientName.c_str(), m_asynClient->is_connected() ? "true" : "false", m_synClient->is_connected() ? "true" : "false");
      sendServiceStatus(CONNECTED_MSG);
    }
    catch (const ::mqtt::exception& e)
    {
      logError("Error to connect with the Mqtt server, reason: {}", e.get_message().c_str());
      return fty::unexpected(to_string(ComState::COM_STATE_CONNECT_FAILED));
    }
    catch (const std::exception& e)
    {
      logError("unexpected error: {}", e.what());
      return fty::unexpected(to_string(ComState::COM_STATE_CONNECT_FAILED));
    }

    return {};
  }

  fty::Expected<void> MsgBusMqtt::receive(const std::string& address, MessageListener messageListener)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logError("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    logDebug("Waiting to receive msg from: {}", address);
    m_cb.setSubscriptions(address, messageListener);
    m_asynClient->set_message_callback([this](::mqtt::const_message_ptr msg) {
      // Wrapper from mqtt msg to Message
      m_cb.onMessageArrived(msg);
    });

    if (!m_asynClient->subscribe(address, QOS)->wait_for(TIMEOUT))
    {
      logError("Receive (Rejected)");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }

    logDebug("Waiting to receive msg from: {} Accepted", address);

    return {};
  }

  fty::Expected<void> MsgBusMqtt::unreceive(const std::string& address)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logError("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    logTrace("{} - unsubscribed on '{}'", m_clientName, address);
    if (!m_asynClient->unsubscribe(address)->wait_for(TIMEOUT))
    {
      logError("Unsubscribed (Rejected)");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }

    m_cb.eraseSubscriptions(address);

    return {};
  }

  fty::Expected<void> MsgBusMqtt::send(const Message& message)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logError("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    logDebug("Sending message {}", message.toString());

    // Adding all meta data inside mqtt properties
    auto props = getMqttPropertiesFromMetaData(message.metaData());

    auto msgToSend = ::mqtt::message_ptr_builder()
                    .topic(message.to())
                    .payload(message.userData())
                    .qos(QOS)
                    .properties(props)
                    .retained(RETAINED)
                    .finalize();

    if (!m_asynClient->publish(msgToSend)->wait_for(TIMEOUT))
    {
      logError("Message sent (Rejected)");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }

    logDebug("Message sent (Accepted)");
    return {};
  }

  fty::Expected<Message> MsgBusMqtt::request(const Message& message, int receiveTimeOut)
  {
    try
    {
      if (!(isServiceAvailable(m_asynClient) && isServiceAvailable(m_synClient)))
      {
        logError("Service not available");
        return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
      }

      ::mqtt::const_message_ptr msg;
      m_synClient->subscribe(message.replyTo(), QOS);
      send(message);

      auto messageArrived = m_synClient->try_consume_message_for(&msg, std::chrono::seconds(receiveTimeOut));
      m_synClient->unsubscribe(message.replyTo());
      if (!messageArrived)
      {
        logError("No message arrive in time!");
        return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_TIMEOUT));
      }

      logDebug("Message arrived ({})", msg->get_payload_str().c_str());
      return Message(getMetaDataFromMqttProperties(msg->get_properties()), msg->get_payload_str());
    }
    catch (std::exception& e)
    {
      return fty::unexpected(e.what());
    }
  }

  void MsgBusMqtt::sendServiceStatus(const std::string& message)
  {
    if (isServiceAvailable(m_asynClient))
    {
      auto topic = DISCOVERY_TOPIC + m_clientName + DISCOVERY_TOPIC_SUBJECT;
      auto msg = ::mqtt::message_ptr_builder()
                   .topic(topic)
                   .payload(message)
                   .qos(::mqtt::ReasonCode::GRANTED_QOS_2)
                   .retained(true)
                   .finalize();
      bool status = m_asynClient->publish(msg)->wait_for(TIMEOUT);
      logInfo("Service status {} => {} [%d]", m_clientName.c_str(), message.c_str(), status);
    }
  }

} // namespace fty::messagebus::mqtt
