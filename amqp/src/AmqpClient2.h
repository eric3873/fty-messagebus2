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
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/MessageBusStatus.h>

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <future>
#include <map>

namespace fty::messagebus::amqp
{

  class AmqpClient2 : public proton::messaging_handler
  {
  public:

    AmqpClient2(const std::string& url, const std::string& address = {}, const std::string& filter = {}, MessageListener messageListener = {});

    ~AmqpClient2() = default;

    void on_container_start(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_sender_open(proton::sender& sender) override;
    void on_receiver_open(proton::receiver& receiver) override;
    void on_message(proton::delivery& delivery, proton::message& msg) override;

    fty::messagebus::ComState connected();

    bool tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout);
    fty::messagebus::DeliveryState receive(const std::string& address, MessageListener messageListener, const std::string& filter = {});
    fty::messagebus::DeliveryState send(const proton::message& msg);
    void close();

  private:
    std::string m_url;
    std::string m_address;
    std::string m_filter;
    MessageListener m_messageListener;

    fty::messagebus::ComState m_communicationState = fty::messagebus::ComState::COM_STATE_UNKNOWN;

    // Synchronization
    std::mutex m_lock;
    std::condition_variable m_senderReady;
    std::promise<proton::message> m_promise;
    std::future<proton::message> m_future;

    std::promise<void> m_promise2;
    std::future<void> m_future2;

    std::promise<fty::messagebus::ComState> m_connectPromise;
    std::future<fty::messagebus::ComState> m_connectFuture;

    std::map<std::string, MessageListener> m_receiverListener;

    // Sender
    proton::sender m_sender;
    // Receiver
    proton::receiver m_receiver;

    proton::connection m_connection;

    proton::receiver_options getReceiverOptions(const std::string& selector_str) const;

    proton::message m_message;
  };

} // namespace fty::messagebus::amqp
