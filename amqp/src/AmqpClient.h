/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#pragma once

#include "MsgBusAmqpUtils.h"
#include <fty/messagebus/MessageBus.h>

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

namespace fty::messagebus::amqp
{

  class AmqpClient : public proton::messaging_handler
  {
  public:

    AmqpClient(const std::string& url, const std::string& address, const std::string& filter = {}, MessageListener messageListener = {});

    ~AmqpClient() = default;

    void on_container_start(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_sender_open(proton::sender& sender) override;
    void on_receiver_open(proton::receiver& receiver) override;
    void on_message(proton::delivery& delivery, proton::message& msg) override;

    bool tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout);
    void send(const proton::message& msg);
    void close();

  private:
    std::string m_url;
    std::string m_address;
    std::string m_filter;
    MessageListener m_messageListener;

    // Synchronization
    std::mutex m_lock;
    std::condition_variable m_senderReady;
    std::promise<proton::message> m_promise;
    std::future<proton::message> m_future;
    // Sender
    proton::sender m_sender;
    // Receiver
    proton::receiver m_receiver;

    proton::source_options setFilter(const std::string& selector_str);
  };

} // namespace fty::messagebus::amqp
