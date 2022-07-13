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

#pragma once

#include "../../../../src/AmqpClient.h"
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty/messagebus2/utils.h>

namespace fty::messagebus2::amqp {

// Default amqp end point
static auto constexpr DEFAULT_ENDPOINT{"amqp://127.0.0.1:5672"};
static auto constexpr BUS_IDENTITY{"AMQP"};

class MessageBusAmqp final : public MessageBus
{
public:
    MessageBusAmqp(const std::string& clientName = utils::getClientId("MessageBusAmqp"), const Endpoint& endpoint = DEFAULT_ENDPOINT);
    ~MessageBusAmqp() = default;

    MessageBusAmqp(MessageBusAmqp&&) = delete;
    MessageBusAmqp& operator = (MessageBusAmqp&&) = delete;
    MessageBusAmqp(const MessageBusAmqp&) = delete;
    MessageBusAmqp& operator = (const MessageBusAmqp&) = delete;

    [[nodiscard]] fty::Expected<void, ComState>      connect() noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState> send(const Message& msg) noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState> receive(
        const Address& address, MessageListener&& messageListener, const std::string& filter = {}) noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState> unreceive(const Address& address) noexcept override;
    void setConnectionErrorListener(ConnectionErrorListener errorListener = {});
    // Sync request with timeout
    [[nodiscard]] fty::Expected<Message, DeliveryState> request(const Message& message, int timeoutInSeconds) noexcept override;
    [[nodiscard]] const ClientName& clientName() const noexcept override;
    [[nodiscard]] const Identity& identity() const noexcept override;

private:
    // Test if the service is available or not
    bool isServiceAvailable();

    // Client name
    std::string m_clientName{};
    // Amqp endpoint
    Endpoint    m_endpoint{};
    // AmqpClient instance
    AmqpClientPointer m_clientPtr;
};

} // namespace fty::messagebus2::amqp
