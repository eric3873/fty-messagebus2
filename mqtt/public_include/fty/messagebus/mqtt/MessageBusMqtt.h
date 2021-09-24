/*  =========================================================================
    MessageBusMqtt.hpp - class description

    Copyright (C) 2014 - 2020 Eaton

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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/utils.h>

namespace fty::messagebus::mqtt
{
  // Default mqtt end point
  static auto constexpr DEFAULT_MQTT_ENDPOINT{"tcp://localhost:1883"};

  static auto constexpr BUS_INDENTITY_MQTT{"MQTT"};

  /*static auto constexpr SECURE_MQTT_END_POINT{"tcp://localhost:8883"};

  // Mqtt default delimiter
  static auto constexpr MQTT_DELIMITER{'/'};

  // Topic
  static const std::string PREFIX_TOPIC = "/etn/t";

  // Queues
  static const std::string PREFIX_QUEUE = "/etn/q/";
  static const std::string PREFIX_REQUEST_QUEUE = PREFIX_QUEUE + "request";
  static const std::string PREFIX_REPLY_QUEUE = PREFIX_QUEUE + "reply";*/

  class MsgBusMqtt;

  class MessageBusMqtt final : public fty::messagebus::MessageBus
  {
  public:
    MessageBusMqtt( const ClientName& clientName = utils::getClientId("MessageBusMqtt"),
                    const Endpoint& endpoint = DEFAULT_MQTT_ENDPOINT/*,
                    const std::string& connectionStateTopic = "",
                    bool reportConnectionState = true*/);


    ~MessageBusMqtt();

    MessageBusMqtt(MessageBusMqtt && other) = default;
    MessageBusMqtt& operator=(MessageBusMqtt&& other) = delete;
    MessageBusMqtt(const MessageBusMqtt& other) = default;
    MessageBusMqtt& operator=(const MessageBusMqtt& other) = delete;

    [[nodiscard]] fty::Expected<void> connect() noexcept override;
    [[nodiscard]] fty::Expected<void> send(const Message& msg) noexcept override;
    [[nodiscard]] fty::Expected<void> subscribe(const std::string& address, std::function<void(const Message&)>&& func) noexcept override;
    [[nodiscard]] fty::Expected<void> unsubscribe(const std::string& address) noexcept override;
    [[nodiscard]] fty::Expected<Message> request(const Message& msg, int timeOut) noexcept override;

    [[nodiscard]] const ClientName & clientName() const noexcept override;
    [[nodiscard]] const Identity & identity() const noexcept override;

  private:
    std::shared_ptr<MsgBusMqtt> m_busMqtt;
  };
} // namespace fty::messagebus::mqtt
