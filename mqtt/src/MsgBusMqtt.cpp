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
    fty_common_messagebus_mqtt -
@discuss
@end
*/

#include "MsgBusMqtt.h"
#include "MsgBusMqttUtils.h"

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
}

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

      logInfo("{} => connect status: sync client: {}, async client: {}", m_clientName.c_str(), m_asynClient->is_connected() ? "true" : "false",  m_synClient->is_connected() ? "true" : "false");
      sendServiceStatus(CONNECTED_MSG);
    }
    catch (const ::mqtt::exception& e)
    {
      logError("Error to connect with the Mqtt server, reason: {}", e.get_message().c_str());
      return fty::unexpected(COM_STATE_CONNECT_FAILED);
    }
    catch (const std::exception& e)
    {
      logError("unexpected error: {}", e.what());
      return fty::unexpected(COM_STATE_CONNECT_FAILED);
    }

    return {};
  }

  fty::Expected<void> MsgBusMqtt::publish(const std::string& topic, const Message& message)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logDebug("Publishing on topic: {}", topic.c_str());
    // Adding all meta data inside mqtt properties
    auto props = getMqttPropertiesFromMetaData(message.metaData());
    // Build the message
    auto pubMsg = ::mqtt::message_ptr_builder()
                    .topic(topic)
                    .payload(message.userData())
                    .qos(QOS)
                    .properties(props)
                    .retained(false)
                    .finalize();
    // Publish it

    if(! m_asynClient->publish(pubMsg)->wait_for(TIMEOUT)) {
      //message rejected
      logDebug("Message rejected");
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Message published (Accepted)");

    return {};
  }

  fty::Expected<void> MsgBusMqtt::subscribe(const std::string& topic, MessageListener messageListener)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }


    logDebug("Subscribing on topic: {}", topic.c_str());
    m_cb.setSubscriptions(topic, messageListener);
    m_asynClient->set_message_callback([this](::mqtt::const_message_ptr msg) {
      // Wrapper from mqtt msg to Message
      m_cb.onMessageArrived(msg);
    });

    if(! m_asynClient->subscribe(topic, QOS)->wait_for(TIMEOUT)) {
      logDebug("Subscribed ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Subscribed (Accepted)");

    return {};
  }

  fty::Expected<void> MsgBusMqtt::unsubscribe(const std::string& topic)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logTrace("{} - unsubscribed for topic '{}'", m_clientName.c_str(), topic.c_str());
    if(! m_asynClient->unsubscribe(topic)->wait_for(TIMEOUT))
    {
      logDebug("Unsubscribed ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    m_cb.eraseSubscriptions(topic);

    return {};
  }

  fty::Expected<void> MsgBusMqtt::receive(const std::string& queue, MessageListener messageListener)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    m_cb.setSubscriptions(queue, messageListener);
    m_asynClient->set_message_callback([this](::mqtt::const_message_ptr msg) {
      const ::mqtt::properties& props = msg->get_properties();
      if (props.contains(::mqtt::property::CORRELATION_DATA))
      {
        // Wrapper from mqtt msg to Message
        m_cb.onMessageArrived(msg, m_asynClient);
      }
      else
      {
        logError("Missing mqtt properties for Req/Rep (i.e. CORRELATION_DATA or RESPONSE_TOPIC");
      }
    });

    if(! m_asynClient->subscribe(queue, QOS)->wait_for(TIMEOUT)) {
      logDebug("Waiting to receive msg from: {} {}", queue.c_str(), DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Waiting to receive msg from: {} Accepted");
    return {};
  }

  fty::Expected<void> MsgBusMqtt::sendRequest(const std::string& requestQueue, const Message& message)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logDebug("Request sent to {}", requestQueue.c_str() );

    // Adding all meta data inside mqtt properties
    auto props = getMqttPropertiesFromMetaData(message.metaData());

    std::string replyTo = "<none>";
    if(message.needReply()){
      replyTo = ::mqtt::get<std::string>(props, ::mqtt::property::RESPONSE_TOPIC);
    }

    logDebug("Sending request payload: '{}' to: {} and wait message on reply queue {}", message.userData().c_str(), requestQueue.c_str(), replyTo.c_str());

    auto reqMsg = ::mqtt::message_ptr_builder()
                    .topic(requestQueue)
                    .payload(message.userData())
                    .qos(QOS)
                    .properties(props)
                    .retained(RETAINED)
                    .finalize();

    if(! m_asynClient->publish(reqMsg)->wait_for(TIMEOUT) ){
      logDebug("Request sent ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Request sent (Accepted)");
    return {};
  }

  fty::Expected<void> MsgBusMqtt::sendRequest(const std::string& requestQueue, const Message& message, MessageListener messageListener)
  {
    auto delivState = receive(requestQueue, messageListener);
    if (!delivState)
    {
      return fty::unexpected(delivState.error());
    }
    return sendRequest(requestQueue, message);
  }

  fty::Expected<void> MsgBusMqtt::sendReply(const std::string& replyQueue, const Message& message)
  {
    if (!isServiceAvailable(m_asynClient))
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    // Adding all meta data inside mqtt properties
    auto props = getMqttPropertiesFromMetaData(message.metaData());

    logDebug("Reply queue: '{}'", replyQueue.c_str());
    logDebug("Sending reply payload: '{}' to: {}", message.userData().c_str(), (::mqtt::get<std::string>(props, ::mqtt::property::RESPONSE_TOPIC)).c_str());
    auto replyMsg = ::mqtt::message_ptr_builder()
                      .topic(replyQueue)
                      .payload(message.userData())
                      .qos(QOS)
                      .properties(props)
                      .retained(RETAINED)
                      .finalize();

    if(! m_asynClient->publish(replyMsg)->wait_for(TIMEOUT) ) {
      logDebug("Reply sent ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Reply sent (Accepted)");

    return {};
  }

  fty::Expected<Message> MsgBusMqtt::request(const std::string& requestQueue, const Message& message, int receiveTimeOut)
  {
    try{
      if (! (isServiceAvailable(m_asynClient) && isServiceAvailable(m_synClient))) {
        logDebug("Service not available");
        return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
      }

      Message replyMsg;

      ::mqtt::const_message_ptr msg;
      auto replyQueue = getReplyQueue(message);

      m_synClient->subscribe(replyQueue, QOS);
      sendRequest(requestQueue, message);

      auto messageArrived = m_synClient->try_consume_message_for(&msg, std::chrono::seconds(receiveTimeOut));
      m_synClient->unsubscribe(replyQueue);
      if (!messageArrived)
      {
        logDebug("No message arrive in time!");
        return fty::unexpected(DELIVERY_STATE_TIMEOUT);
      }

      logDebug("Message arrived ({})", msg->get_payload_str().c_str());
      return Message(getMetaDataFromMqttProperties(msg->get_properties()), msg->get_payload_str());
    }
    catch (std::exception & e)
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
