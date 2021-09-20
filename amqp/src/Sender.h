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
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <future>
#include <iostream>
#include <vector>

#include <fty_log.h>

namespace fty::messagebus::amqp
{

  class Sender : public proton::messaging_handler
  {
  private:
    std::string m_url;
    std::string m_address;
    proton::connection m_connection;

    // Synchronization
    std::mutex m_lock;
    std::condition_variable m_cv;
    std::condition_variable m_cvSenderReady;
    // sender
    proton::sender m_sender;
    proton::work_queue* p_workQueue;

  public:
    Sender(const std::string& url, const std::string& address)
      : m_url(url)
      , m_address(address)
    {
    }

    ~Sender()
    {
      //cancel();
    }

    void on_container_start(proton::container& con) override
    {
      logDebug("Sender on_container_start");
      try
      {
        m_connection = con.connect(m_url);
      }
      catch(std::exception& e)
      {
      log_error("Exception {}", e.what());
      }
    }

    void on_connection_open(proton::connection& conn) override
    {
      logDebug("Sender on_connection_open for target address: {}", m_address.c_str());
      conn.open_sender(m_address);
    }

    void on_sender_open(proton::sender& s) override
    {
      // sender_ and work_queue_ must be set atomically
      logDebug("on_sender_open");
      std::unique_lock<std::mutex> l(m_lock);
      m_sender = s;
      //p_workQueue = &s.work_queue();
      m_cvSenderReady.notify_all();
    }

    void sendMsg(const proton::message& msg)
    {
      logDebug("Init sender waiting...");
      std::unique_lock<std::mutex> l(m_lock);
      //m_cv.wait(l);
      // logDebug("connection is ready");
      // m_connection.open_sender(msg.to());
      m_cvSenderReady.wait(l);
      logDebug("sender ready on {}", msg.to().c_str());
      // if (p_workQueue)
      // {


      m_sender.work_queue().add([=]() {
        auto tracker = m_sender.send(msg);
        logDebug("Msg sent {}", proton::to_string(tracker.state()));
        m_sender.connection().close();
        logDebug("Sender closed");
      });


      //}
      // auto tracker = m_sender.send(msg);
      // logDebug("Msg sent {}", proton::to_string(tracker.state()).c_str());
      // m_sender.connection().close();
      // logDebug("Sender closed");
      // l.unlock();
      // cancel();

      // l.unlock();
      // work_queue()->add([=]() {
      //   std::cout << "send msg" << std::endl;
      //   m_sender.send(msg);
      //   cancel();
      // });
    }

    void cancel()
    {
      std::lock_guard<std::mutex> l(m_lock);
      logDebug("Cancel for {}", m_address.c_str());
      if (m_sender)
      {
        m_sender.connection().close();
      }
      logDebug("Canceled");
    }

  private:
    proton::work_queue* work_queue()
    {
      logDebug("work_queue");
      // Wait till work_queue_ and sender_ are initialized.
      std::unique_lock<std::mutex> l(m_lock);
      logDebug("work_queue2");

      while (!p_workQueue)
      {
        logDebug("in while");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        m_cvSenderReady.wait(l);
      }
      logDebug("queue ok");
      return p_workQueue;
    }
  };

} // namespace fty::messagebus::amqp
