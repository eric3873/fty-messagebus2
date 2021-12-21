/*  =========================================================================
    CallBack.cpp - class description

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
    CallBack.cpp -
@discuss
@end
*/

#include "CallBack.h"
#include <fty/messagebus/Message.h>

#include <fty_log.h>

#include <mqtt/async_client.h>
#include <mqtt/properties.h>

namespace
{

  using namespace fty::messagebus;

  static auto getMetaDataFromMqttProperties(const ::mqtt::properties& props) -> const MetaData
  {

    logDebug("getMetaDataFromMqttProperties...");
    auto metaData = MetaData{};

    // User properties
    if (props.contains(::mqtt::property::USER_PROPERTY))
    {
      std::string key, value;
      for (size_t i = 0; i < props.count(::mqtt::property::USER_PROPERTY); i++)
      {
        std::tie(key, value) = ::mqtt::get<::mqtt::string_pair>(props, ::mqtt::property::USER_PROPERTY, i);
        metaData.emplace(key, value);
      }
    }
    // Req/Rep pattern properties
    if (props.contains(::mqtt::property::CORRELATION_DATA))
    {
      metaData.emplace(CORRELATION_ID, ::mqtt::get<std::string>(props, ::mqtt::property::CORRELATION_DATA));
    }

    if (props.contains(::mqtt::property::RESPONSE_TOPIC))
    {
      metaData.emplace(REPLY_TO, ::mqtt::get<std::string>(props, ::mqtt::property::RESPONSE_TOPIC));
    }

    logDebug("Done.");
    return metaData;
  }

} // namespace

namespace fty::messagebus::mqtt
{
  size_t NB_WORKERS = 16;

  CallBack::CallBack()
    : ::mqtt::callback()
  {
    m_poolWorkers = std::make_shared<utils::PoolWorker>(NB_WORKERS);
  }

  // Callback called when connection lost.
  void CallBack::connection_lost(const std::string& cause)
  {
    logError("Connection lost");
    if (!cause.empty())
    {
      logError("raison: {}", cause);
    }
  }

  // Callback called for connection done.
  void CallBack::onConnected(const std::string& cause)
  {
    logDebug("Connected");
    if (!cause.empty())
    {
      logDebug("raison: {}", cause);
    }
  }

  // Callback called for connection updated.
  bool CallBack::onConnectionUpdated(const ::mqtt::connect_data& /*connData*/)
  {
    logInfo("Connection updated");
    return true;
  }

  auto CallBack::getSubscriptions() -> SubScriptionListener
  {
    return m_subscriptions;
  }

  auto CallBack::subscribed(const std::string& topic) -> bool
  {
    bool isSubscript = false;
    if (auto iter{m_subscriptions.find(topic)}; iter != m_subscriptions.end())
    {
      isSubscript = true;
    }
    return isSubscript;
  }

  void CallBack::setSubscriptions(const std::string& topic, MessageListener messageListener)
  {
    if (auto it{m_subscriptions.find(topic)}; it == m_subscriptions.end())
    {
      m_subscriptions.emplace(topic, messageListener);
    }
  }

  void CallBack::eraseSubscriptions(const std::string& topic)
  {
    m_subscriptions.erase(topic);
  }

  // Callback called when a mqtt message arrives.
  void CallBack::onMessageArrived(::mqtt::const_message_ptr msg, AsynClientPointer clientPointer)
  {
    auto topic = msg->get_topic();
    logTrace("Message received from topic: '{}'", topic);
    // build metaData message from mqtt properties
    auto metaData = getMetaDataFromMqttProperties(msg->get_properties());
    if (auto it{m_subscriptions.find(topic)}; it != m_subscriptions.end())
    {
      try
      {
        // Delegate to the pool worker
        logTrace("Notify received from topic: '{}'", topic);
        m_poolWorkers->offload([this, clientPointer, topic](MessageListener listener, const Message& mqttMsg) {
          if (listener)
          {
            logTrace("Trigger callback...");
            listener(mqttMsg);
            logTrace("Trigger callback... Done.");
          }
          else
          {
            logTrace("No callback to trigger");
          }

          // Unsubscribe only reply
          auto iterator = mqttMsg.metaData().find(SUBJECT);
          if (clientPointer && (iterator != mqttMsg.metaData().end()))
          {
            clientPointer->unsubscribe(topic);
            this->eraseSubscriptions(topic);
          }
        },(it->second), Message{metaData, msg->get_payload_str()});
      }
      catch (const std::exception& e)
      {
        logError("Error in listener of queue '{}': '{}'", it->first, e.what());
      }
      catch (...)
      {
        logError("Error in listener of queue '{}': 'unknown error'", it->first);
      }
    }
    else
    {
      logWarn("Message skipped for {}", topic);
    }
  }

} // namespace fty::messagebus::mqtt
