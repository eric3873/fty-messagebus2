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

#include "fty/messagebus2/amqp/MessageBusAmqp.h"
#include "MsgBusAmqpUtils.h"
#include "fty/messagebus2/Promise.h"
#include <fty/thread-pool.h>
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
#include <condition_variable>

namespace fty::messagebus2::amqp {

using MessageListener      = fty::messagebus2::MessageListener;
using SubScriptionListener = std::map<Address, MessageListener>;
class AmqpClient;
using AmqpClientPointer = std::shared_ptr<AmqpClient>;

class AmqpClient : public proton::messaging_handler
{
public:
    AmqpClient(const Endpoint& url);
    ~AmqpClient();

    // proton::messaging_handler Callback
    void on_container_start(proton::container& container) override;
    void on_container_stop(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_connection_close(proton::connection& connection) override;
    void on_connection_error(proton::connection& connection) override;
    void on_session_open(proton::session&) override;
    void on_session_close(proton::session&) override;
    void on_sender_open(proton::sender& sender) override;
    void on_sender_close(proton::sender& sender) override;
    void on_receiver_open(proton::receiver& receiver) override;
    void on_receiver_close(proton::receiver& receiver) override;
    void on_error(const proton::error_condition& error) override;
    void on_transport_error(proton::transport& t) override;
    void on_transport_open(proton::transport&) override;
    void on_transport_close(proton::transport&) override;
    void on_message(proton::delivery&, proton::message& msg) override;

    bool isConnected();
    fty::messagebus2::ComState connected();
    fty::messagebus2::DeliveryState send(const proton::message& msg);
    fty::messagebus2::DeliveryState receive(
        const Address& address, MessageListener messageListener = {}, const std::string& filter = {});
    fty::messagebus2::DeliveryState unreceive(const Address& address, const std::string& filter = {});
    void close();

private:
    Endpoint                m_url;
    SubScriptionListener    m_subscriptions;

    // Default communication state
    fty::messagebus2::ComState m_communicationState = fty::messagebus2::ComState::Unknown;

    // Proton object
    proton::connection    m_connection;
    proton::receiver      m_receiver;
    proton::message       m_message;

    // Pool thread
    fty::ThreadPool *m_pool { nullptr };

    // Mutex
    std::mutex m_lock;
    std::mutex m_lockMain;

    // Set of promise for synchronization
    Promise<fty::messagebus2::ComState> m_connectPromise;
    Promise<void>                       m_promiseSender;
    Promise<void>                       m_promiseReceiver;
    Promise<void>                       m_promiseSenderClose;

    // Container stop
    bool                                m_containerStopOk {false};
    std::condition_variable             m_cvContainerStop;
    std::mutex                          m_lockContainerStop;

    // Connection close
    std::condition_variable             m_cvConnectionClose;
    std::mutex                          m_lockConnectionClose;

protected:
    void resetPromises();
    std::string getAddressSubscriptions(const std::string& filter);
    bool setSubscriptions(const std::string& key, MessageListener messageListener);
    bool unsetSubscriptions(const std::string& key);
};

} // namespace fty::messagebus2::amqp
