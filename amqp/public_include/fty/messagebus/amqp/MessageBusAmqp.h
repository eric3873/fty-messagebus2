/*  =========================================================================
    MessageBusAmqp.h - class description

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

namespace fty::messagebus::amqp
{
  // Default amqp end point
  static auto constexpr DEFAULT_AMQP_END_POINT{"amqp://127.0.0.1:5672"};

  static auto constexpr BUS_INDENTITY_AMQP{"AMQP"};

  class MsgBusAmqp;

  class MessageBusAmqp final : public fty::messagebus::MessageBus
  {
  public:
    MessageBusAmqp(const ClientName& clientName = utils::getClientId("MessageBusAmqp"),
                   const Endpoint& endpoint = DEFAULT_AMQP_END_POINT);

    ~MessageBusAmqp();

    MessageBusAmqp(MessageBusAmqp&& other) = default;
    MessageBusAmqp& operator=(MessageBusAmqp&& other) = delete;
    MessageBusAmqp(const MessageBusAmqp& other) = default;
    MessageBusAmqp& operator=(const MessageBusAmqp& other) = delete;

    [[nodiscard]] fty::Expected<void> connect() noexcept override;
    [[nodiscard]] fty::Expected<void> send(const Message& msg) noexcept override;
    [[nodiscard]] fty::Expected<void> subscribe(const std::string& queue, std::function<void(const Message&)>&& func) noexcept override;
    [[nodiscard]] fty::Expected<void> unsubscribe(const std::string& queue) noexcept override;
    [[nodiscard]] fty::Expected<Message> request(const Message& msg, int timeOut) noexcept override;

    [[nodiscard]] const ClientName& clientName() const noexcept override;
    [[nodiscard]] const Identity& identity() const noexcept override;

  private:
    std::shared_ptr<MsgBusAmqp> m_busAmqp;
  };

} // namespace fty::messagebus::amqp
