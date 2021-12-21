/*  =========================================================================
    MessageBusMqtt.hpp - class description

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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/utils.h>

namespace fty::messagebus::mqtt
{
  // Default mqtt end point
  static auto constexpr DEFAULT_ENDPOINT{"tcp://localhost:1883"};
  static auto constexpr BUS_IDENTITY{"MQTT"};

  static auto constexpr QOS{"QOS"};
  static auto constexpr RETAIN{"RETAIN"};

  class MsgBusMqtt;

  class MessageBusMqtt final : public fty::messagebus::MessageBus
  {
  public:
    MessageBusMqtt( const ClientName& clientName = utils::getClientId("MessageBusMqtt"),
                    const Endpoint& endpoint = DEFAULT_ENDPOINT,
                    const Message& will = {});


    ~MessageBusMqtt();

    MessageBusMqtt(MessageBusMqtt && other) = delete;
    MessageBusMqtt& operator=(MessageBusMqtt&& other) = delete;
    MessageBusMqtt(const MessageBusMqtt& other) = delete;
    MessageBusMqtt& operator=(const MessageBusMqtt& other) = delete;

    [[nodiscard]] fty::Expected<void> connect() noexcept override;
    [[nodiscard]] fty::Expected<void> send(const Message& msg) noexcept override;
    [[nodiscard]] fty::Expected<void> receive(const Address& address, MessageListener&& func, const std::string& filter = {}) noexcept override;
    [[nodiscard]] fty::Expected<void> unreceive(const Address& address) noexcept override;
    [[nodiscard]] fty::Expected<Message> request(const Message& msg, int timeOut) noexcept override;

    [[nodiscard]] const ClientName & clientName() const noexcept override;
    [[nodiscard]] const Identity & identity() const noexcept override;

  private:
    std::shared_ptr<MsgBusMqtt> m_busMqtt;
  };
} // namespace fty::messagebus::mqtt
