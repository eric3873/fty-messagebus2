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

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>

#include <proton/tracker.hpp>

#include <future>

namespace fty::messagebus::amqp
{

  class Sender : public proton::messaging_handler
  {
  private:
    std::string m_url;
    std::string m_address;

    // Synchronization
    std::mutex m_lock;
    std::condition_variable m_cv;
    std::condition_variable m_cvSenderReady;
    // Sender
    proton::sender m_sender;

  public:
    Sender(const std::string& url, const std::string& address)
      : m_url(url)
      , m_address(address)
    {
    }

    ~Sender() = default;

    void on_container_start(proton::container& container) override;
    void on_connection_open(proton::connection& connection) override;
    void on_sender_open(proton::sender& sender) override;

    void sendMsg(const proton::message& msg);
    void close();
  };

} // namespace fty::messagebus::amqp
