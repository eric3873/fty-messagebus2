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

#include "MsgBusAmqpUtils.h"
#include <fty/messagebus2/MessageBus.h>
#include <fty/messagebus2/MessageBusStatus.h>
#include <future>
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>
#include <proton/transport.hpp>
#include <proton/work_queue.hpp>

namespace fty::messagebus2::amqp {

using MessageListener      = fty::messagebus2::MessageListener;
using SubScriptionListener = std::pair<Address, MessageListener>;

class AmqpClient : public proton::messaging_handler
{
public:
    AmqpClient(const Endpoint& url);
    ~AmqpClient();

    // proton::messaging_handler Callback
    void on_container_start(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_sender_open(proton::sender& sender) override;
    void on_receiver_open(proton::receiver& receiver) override;
    void on_receiver_close(proton::receiver&) override;
    void on_message(proton::delivery& delivery, proton::message& msg) override;
    void on_error(const proton::error_condition& error) override;
    void on_transport_error(proton::transport& t) override;

    fty::messagebus2::ComState      connected();
    fty::messagebus2::DeliveryState receive(const Address& address, const std::string& filter = {}, MessageListener messageListener = {});
    fty::messagebus2::DeliveryState unreceive();
    fty::messagebus2::DeliveryState send(const proton::message& msg);
    void                           close();

private:
    Endpoint             m_url;
    SubScriptionListener m_subscriptions;
    // Default communication state
    fty::messagebus2::ComState m_communicationState = fty::messagebus2::ComState::Unknown;
    // Proton object
    proton::connection m_connection;
    proton::receiver   m_receiver;
    proton::message    m_message;
    // Mutex
    std::mutex m_lock;
    // Set of promise for synchronization
    std::promise<fty::messagebus2::ComState> m_connectPromise;
    std::promise<void>                      m_promiseSender;
    std::promise<void>                      m_promiseReceiver;

    void setSubscriptions(const Address& address, MessageListener messageListener);
    void resetPromise();
};

} // namespace fty::messagebus2::amqp
