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

#include "AmqpClient.h"
#include <fty/expected.h>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>

namespace fty::messagebus::amqp {

using MessagePointer    = std::shared_ptr<proton::message>;
using AmqpClientPointer = std::shared_ptr<AmqpClient>;

class MsgBusAmqp
{
public:
    MsgBusAmqp(const std::string& clientName, const Endpoint& endpoint)
        : m_clientName(clientName)
        , m_endpoint(endpoint){};

    MsgBusAmqp() = delete;
    ~MsgBusAmqp();

    MsgBusAmqp(MsgBusAmqp&&) = delete;
    MsgBusAmqp& operator=(MsgBusAmqp&&) = delete;
    MsgBusAmqp(const MsgBusAmqp&)       = delete;
    MsgBusAmqp& operator=(const MsgBusAmqp&) = delete;

    [[nodiscard]] fty::Expected<void, ComState> connect();

    [[nodiscard]] fty::Expected<void, DeliveryState> receive(
        const Address& address, MessageListener messageListener, const std::string& filter = {});
    [[nodiscard]] fty::Expected<void, DeliveryState> unreceive(const Address& address);
    [[nodiscard]] fty::Expected<void, DeliveryState> send(const Message& message);

    // Sync request with timeout
    [[nodiscard]] fty::Expected<Message, DeliveryState> request(const Message& message, int timeoutInSeconds);

    const std::string& clientName() const
    {
        return m_clientName;
    }

    bool isServiceAvailable();

private:
    std::string m_clientName{};
    Endpoint    m_endpoint{};

    // To handle all receivers and theirs message listener
    std::map<std::string, AmqpClientPointer> m_subScriptions;
    // To handle connection, etc.
    AmqpClientPointer m_amqpClient;
};

} // namespace fty::messagebus::amqp
