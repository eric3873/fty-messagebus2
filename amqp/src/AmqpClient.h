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

  class AmqpClient : public proton::messaging_handler
  {
  public:

    AmqpClient(const std::string& url);
    ~AmqpClient();

    void on_container_start(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_sender_open(proton::sender& sender) override;
    void on_receiver_open(proton::receiver& receiver) override;
    void on_message(proton::delivery& delivery, proton::message& msg) override;
    void on_error(const proton::error_condition& error) override;

    fty::messagebus::ComState connected();
    fty::messagebus::DeliveryState receive(const std::string& address, const std::string& filter = {}, MessageListener messageListener = {});
    fty::messagebus::DeliveryState send(const proton::message& msg);
    bool tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout);
    void close();

  private:
    std::string m_url;
    MessageListener m_messageListener;
    // Proton connection
    proton::connection m_connection;
    // Proton message
    proton::message m_message;
    // Default communication state
    fty::messagebus::ComState m_communicationState = fty::messagebus::ComState::COM_STATE_UNKNOWN;

    // Synchronization
    std::mutex m_lock;

    // Set of promise
    std::promise<fty::messagebus::ComState> m_connectPromise;
    std::future<fty::messagebus::ComState> m_connectFuture;
    std::promise<void> m_promiseSender;
    std::promise<void> m_promiseReceiver;
    std::promise<proton::message> m_promiseSyncRequest;

    proton::receiver_options getReceiverOptions(const std::string& selector_str) const;
  };

} // namespace fty::messagebus::amqp
