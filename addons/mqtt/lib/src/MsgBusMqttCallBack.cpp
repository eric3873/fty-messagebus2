/*  =========================================================================
    fty_common_messagebus_mqtt - class description

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
    MsgBusMqttCallBack.cpp -
@discuss
@end
*/

#include "fty/messagebus/mqtt/MsgBusMqttCallBack.hpp"
#include "fty/messagebus/mqtt/MsgBusMqttMessage.hpp"

#include <fty_log.h>

#include <mqtt/async_client.h>
#include <mqtt/properties.h>

namespace
{

  using namespace fty::messagebus;

  static auto getMetaDataFromMqttProperties(const mqtt::properties& props) -> const MetaData
  {
    auto metaData = MetaData{};

    // User properties
    if (props.contains(mqtt::property::USER_PROPERTY))
    {
      std::string key, value;
      for (size_t i = 0; i < props.count(mqtt::property::USER_PROPERTY); i++)
      {
        std::tie(key, value) = mqtt::get<mqtt::string_pair>(props, mqtt::property::USER_PROPERTY, i);
        metaData.emplace(key, value);
      }
    }
    // Req/Rep pattern properties
    if (props.contains(mqtt::property::CORRELATION_DATA))
    {
      metaData.emplace(CORRELATION_ID, mqtt::get<std::string>(props, mqtt::property::CORRELATION_DATA));
    }

    if (props.contains(mqtt::property::RESPONSE_TOPIC))
    {
      metaData.emplace(REPLY_TO, mqtt::get<std::string>(props, mqtt::property::RESPONSE_TOPIC));
    }
    return metaData;
  }

} // namespace

namespace fty::messagebus::mqttv5
{
  size_t NB_WORKERS = 16;

  CallBack::CallBack()
  {
    m_poolWorkers = std::make_shared<utils::PoolWorker>(NB_WORKERS);
  }

  // Callback called when connection lost.
  void CallBack::connection_lost(const std::string& cause)
  {
    log_error("Connection lost");
    if (!cause.empty())
    {
      log_error("raison: %s", cause.c_str());
    }
  }

  // Callback called for connection done.
  void CallBack::onConnected(const std::string& cause)
  {
    log_debug("Connected");
    if (!cause.empty())
    {
      log_debug("raison: %s", cause.c_str());
    }
  }

  // Callback called for connection updated.
  bool CallBack::onConnectionUpdated(const mqtt::connect_data& /*connData*/)
  {
    log_info("Connection updated");
    return true;
  }

  auto CallBack::getSubscriptions() -> SubScriptionListener
  {
    return m_subscriptions;
  }

  void CallBack::setSubscriptions(const std::string& topic, MessageListener messageListener)
  {
    if (auto it{m_subscriptions.find(topic)}; it == m_subscriptions.end())
    {
      auto ret = m_subscriptions.emplace(topic, messageListener);
      log_trace("Subscriptions emplaced: %s %s", topic.c_str(), ret.second ? "true" : "false");
    }
  }

  void CallBack::eraseSubscriptions(const std::string& topic)
  {
    auto eraseNbr = m_subscriptions.erase(topic);
    log_trace("Subscriptions deleted: %s %s", topic.c_str(), eraseNbr ? "true" : "false");
  }

  // Callback called when a mqtt message arrives.
  void CallBack::onMessageArrived(mqtt::const_message_ptr msg, AsynClientPointer clientPointer)
  {
    auto topic = msg->get_topic();
    log_trace("Message received from topic: '%s'", topic.c_str());
    // build metaData message from mqtt properties
    auto metaData = getMetaDataFromMqttProperties(msg->get_properties());
    if (auto it{m_subscriptions.find(topic)}; it != m_subscriptions.end())
    {
      try
      {
        // Delegate to the pool worker
        m_poolWorkers->offload([this, clientPointer, topic](MessageListener listener, const MqttMessage& mqttMsg) {
          listener(mqttMsg);
          // Unsubscribe only reply
          auto iterator = mqttMsg.metaData().find(SUBJECT);
          if (clientPointer && (iterator != mqttMsg.metaData().end() && iterator->second == ANSWER_USER_PROPERTY))
          {
            clientPointer->unsubscribe(topic);
            this->eraseSubscriptions(topic);
          }
        },(it->second), MqttMessage{metaData, msg->get_payload_str()});
      }
      catch (const std::exception& e)
      {
        log_error("Error in listener of queue '%s': '%s'", it->first.c_str(), e.what());
      }
      catch (...)
      {
        log_error("Error in listener of queue '%s': 'unknown error'", it->first.c_str());
      }
    }
    else
    {
      log_warning("Message skipped for %s", topic.c_str());
    }
  }

} // namespace fty::messagebus::mqttv5
