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

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>

#include <future>
#include <iostream>
#include <vector>

namespace fty::messagebus::amqp
{
  using proton::receiver_options;
  using proton::source_options;

  class Requester : public proton::messaging_handler
  {
  private:
    std::string m_url;
    proton::message m_request;
    std::promise<proton::message> m_promise;
    std::future<proton::message> m_future;
    std::mutex m_lock;

    proton::connection m_connection;

    proton::sender m_sender;
    proton::receiver m_receiver;

  public:
    Requester(const std::string& url, const proton::message& message)
      : m_url(url)
      , m_request(message)
    {
      m_future = m_promise.get_future();
    }

    void on_container_start(proton::container& con) override
    {
      log_debug("on_container_start");
      m_connection = con.connect(m_url);

      m_sender = m_connection.open_sender(m_request.to());
      // Create a receiver requesting a dynamically created queue
      // for the message source.
      receiver_options opts = receiver_options().source(source_options().dynamic(true));
      m_receiver = m_sender.connection().open_receiver("", opts);
    }

    void send_request()
    {
      log_debug("send_request");
      // TODO see where to set this.
      m_request.reply_to(m_receiver.source().address());
      m_sender.send(m_request);
    }

    bool tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout)
    {
      log_debug("Checking answer for %d, please wait", timeout);

      bool messageArrived = false;
      if (m_future.wait_for(std::chrono::seconds(timeout)) != std::future_status::timeout)
      {
        *resp = std::move(m_future.get());
        messageArrived = true;
      }
      cancel(m_receiver);
      return messageArrived;
    }

    void on_sender_open(proton::sender& s) override
    {
      log_debug("Open sender for target address: %s", s.target().address().c_str());
    }

    void on_receiver_open(proton::receiver& receiver) override
    {
      log_debug("Open receiver for target address: %s", receiver.source().address().c_str());
      send_request();
    }

    void cancel(proton::receiver receiver)
    {
      std::lock_guard<std::mutex> l(m_lock);
      log_debug("Cancel");
      if (m_receiver)
      {
        log_debug("Cancel1");
        m_receiver.connection().close();
      }
      if (m_sender)
      {
        log_debug("Cancel2");
        m_sender.connection().close();
      }
      if (m_connection)
      {
        log_debug("Cancel3");

        m_connection.close();
      }
      log_debug("Canceled");
    }

    void on_message(proton::delivery& d, proton::message& msg) override
    {
      std::lock_guard<std::mutex> l(m_lock);
      m_promise.set_value(msg);
    }
  };

} // namespace fty::messagebus::amqp
