/*  =========================================================================
    Copyright (C) 2014 - 2022 Eaton

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

#include "fty/messagebus2/amqp/MessageBusAmqp.h"

#include <qpid/messaging/Message.h>
#include <qpid/messaging/Connection.h>

#include <atomic>
#include <mutex>

namespace fty::messagebus2::amqp {

class AmqpClient;
using AmqpClientPointer = std::shared_ptr<AmqpClient>;

class AmqpReceiver;
using AmqpReceiverPointer  = std::shared_ptr<fty::messagebus2::amqp::AmqpReceiver>;

static auto constexpr DEFAULT_SESSION {"default_session"};

class AmqpClient
{
public:
    AmqpClient(const ClientName& clientName, const Endpoint& url);
    ~AmqpClient();

    fty::messagebus2::ComState connect();
    bool isConnected();

    fty::messagebus2::DeliveryState receive(const Address& address, MessageListener messageListener, const std::string& filter = {});
    fty::messagebus2::DeliveryState unreceive(const Address& address, const std::string& filter = {});
    fty::messagebus2::DeliveryState send(const qpid::messaging::Message& msg);

    void close();
    bool isClosed()
    {
        return m_closed;
    };

    const std::string getName() { return m_clientName; };
    std::shared_ptr<qpid::messaging::Connection> getConnection() { return m_connection; };

private:
    const ClientName  m_clientName;
    Endpoint          m_url;
    std::atomic<bool> m_closed;

    // Qpid object
    std::shared_ptr<qpid::messaging::Connection> m_connection;

    // List of receivers
    std::vector<fty::messagebus2::amqp::AmqpReceiverPointer> m_receivers;

    // Mutex for receivers list
    std::mutex m_lock;
};

} // namespace fty::messagebus2::amqp
