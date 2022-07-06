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

#include <fty/messagebus2/Message.h>
#include <fty/messagebus2/MessageBus.h>
#include <fty/messagebus2/utils.h>


//#include "AmqpClient.h"
//#include <fty/expected.h>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>
#include <mutex>


namespace fty::messagebus2::amqp {

// Default amqp end point
static auto constexpr DEFAULT_ENDPOINT{"amqp://127.0.0.1:5672"};

static auto constexpr BUS_IDENTITY{"AMQP"};
static const std::string TOPIC_PREFIX = "topic://";
static const std::string QUEUE_PREFIX = "queue://";

//class MsgBusAmqp;

/*class MessageBusAmqp final : public MessageBus
{
public:
    MessageBusAmqp(const ClientName& clientName = utils::getClientId("MessageBusAmqp"), const Endpoint& endpoint = DEFAULT_ENDPOINT);

    ~MessageBusAmqp() = default;

    MessageBusAmqp(MessageBusAmqp&&) = delete;
    MessageBusAmqp& operator=(MessageBusAmqp&&) = delete;
    MessageBusAmqp(const MessageBusAmqp&)       = delete;
    MessageBusAmqp& operator=(const MessageBusAmqp&) = delete;

    [[nodiscard]] fty::Expected<void, ComState>      connect() noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState> send(const Message& msg) noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState> receive(
        const Address& address, MessageListener&& func, const std::string& filter = {}) noexcept override;
    [[nodiscard]] fty::Expected<void, DeliveryState>    unreceive(const Address& address) noexcept override;
    [[nodiscard]] fty::Expected<Message, DeliveryState> request(const Message& msg, int timeOut) noexcept override;

    [[nodiscard]] const ClientName& clientName() const noexcept override;
    [[nodiscard]] const Identity&   identity() const noexcept override;

    using MessageBus::receive;
private:
    std::shared_ptr<MsgBusAmqp> m_busAmqp;
};*/


class AmqpClient;    
using AmqpClientPointer = std::shared_ptr<AmqpClient>;

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

    // Mutex
    //std::mutex m_lock;
};

} // namespace fty::messagebus2::amqp
