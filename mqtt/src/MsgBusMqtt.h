/*  =========================================================================
    MsgBusMqtt.h - class description

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

#pragma once

#include "CallBack.h"

#include <fty/expected.h>

namespace fty::messagebus::mqtt
{
  // Default mqtt end point
  static auto constexpr DEFAULT_MQTT_END_POINT{"tcp://localhost:1883"};
  static auto constexpr SECURE_MQTT_END_POINT{"tcp://localhost:8883"};

  // Mqtt default delimiter
  static auto constexpr MQTT_DELIMITER{'/'};

  // Mqtt will topic
  static auto constexpr DISCOVERY_TOPIC{"/etn/t/service/"};
  static auto constexpr DISCOVERY_TOPIC_SUBJECT{"/status"};

  // Mqtt will message
  static auto constexpr CONNECTED_MSG{"CONNECTED"};
  static auto constexpr DISCONNECTED_MSG{"DISCONNECTED"};
  static auto constexpr DISAPPEARED_MSG{"DISAPPEARED"};

  class MsgBusMqtt
  {
  public:
    MsgBusMqtt() = delete;

    MsgBusMqtt(const std::string& clientName, const std::string& endpoint, const std::string& willTopic = "", const std::string& willMessage = DISAPPEARED_MSG)
      : m_clientName(clientName)
      , m_endpoint(endpoint)
      , m_willTopic(willTopic)
      , m_willMessage(willMessage){};

    ~MsgBusMqtt();

    [[nodiscard]] fty::Expected<void> connect();

    fty::Expected<void> receive(const std::string& address, MessageListener messageListener);
    fty::Expected<void> unreceive(const std::string& address);
    fty::Expected<void> send(const Message& message);

    // Sync request with timeout
    fty::Expected<Message> request(const Message& message, int receiveTimeOut);

    const std::string& clientName() const
    {
      return m_clientName;
    }

  private:
    std::string m_clientName;
    std::string m_endpoint;
    std::string m_willTopic;
    std::string m_willMessage;

    // Asynchronous and synchronous mqtt client
    AsynClientPointer m_asynClient;
    SynClientPointer m_synClient;

    // Call back
    CallBack m_cb;
  };
} // namespace fty::messagebus::mqtt
