/*  =========================================================================
    fty_common_messagebus_mqtt - class description

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

#include <fty_log.h>

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/sender.hpp>
#include <proton/source_options.hpp>
#include <proton/work_queue.hpp>

#include <proton/sender.hpp>
#include <proton/sender_options.hpp>
#include <proton/target.hpp>
#include <proton/target_options.hpp>
#include <proton/transport.hpp>

#include <atomic>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <sstream>
#include <string>
#include <thread>

namespace fty::messagebus::amqp
{

  // Lock output from threads to avoid scrambling
  std::mutex out_lock;
#define OUT(x)                               \
  do                                         \
  {                                          \
    std::lock_guard<std::mutex> l(out_lock); \
    x;                                       \
  } while (false)

  // Handler for a single thread-safe sending and receiving connection.
  class Client : public proton::messaging_handler
  {
    std::string m_url;
    std::string m_senderAddress;
    std::string m_receiverAddress;
    proton::connection m_connection;

    // Only used in proton handler thread
    proton::sender m_sender;
    proton::receiver m_receiver;

    // Shared by proton and user threads, protected by lock_
    std::mutex lock_;
    proton::work_queue* work_queue_;
    std::condition_variable sender_ready_;
    std::queue<proton::message> messages_;
    std::condition_variable messages_ready_;

  public:
    Client(const std::string& url)
      : m_url(url)
      , work_queue_(0)
    {
    }

    ~Client()
    {
      log_debug("Cleaning Amqp client");
      if (m_sender)// && m_sender.active())
      {
        log_debug("Cleaning sender");
        m_sender.connection().close();
      }

      if (m_receiver )//&& m_receiver.active())
      {
        log_debug("Cleaning receiverer");
        m_receiver.connection().close();
      }
      if (m_connection )//&& m_connection.active())
      {
        log_debug("Cleaning connection");
        m_connection.close();
      }
      log_debug("Amqp cleaned");
    }

    // Thread safe
    void send(const proton::message& msg)
    {
      // Use [=] to copy the message, we cannot pass it by reference since it
      // will be used in another thread.
      log_debug("Sending");
      work_queue()->add([=]() { m_sender.send(msg); });
    }

    // Thread safe
    proton::message receive()
    {
      std::unique_lock<std::mutex> l(lock_);
      while (messages_.empty())
        messages_ready_.wait(l);
      auto msg = std::move(messages_.front());
      messages_.pop();
      return msg;
    }

    // Thread safe
    void close()
    {
      work_queue()->add([=]() { m_sender.connection().close(); });
    }

    void senderAddress(const std::string& address)
    {
      m_senderAddress = address;
      if (m_connection)
      {
        //m_connection.open_sender(m_senderAddress);
      }
    }

    void receiverAddress(const std::string& address)
    {
      m_receiverAddress = address;
      if (m_connection)
      {
        m_connection.open_receiver(m_receiverAddress);
      }
    }

    void url(const std::string& urlParam)
    {
      m_url = urlParam;
    }

  private:
    proton::work_queue* work_queue()
    {
      // Wait till work_queue_ and m_sender are initialized.
      std::unique_lock<std::mutex> l(lock_);
      while (!work_queue_)
        sender_ready_.wait(l);
      return work_queue_;
    }

    // == messaging_handler overrides, only called in proton handler thread

    // Note: this example creates a connection when the container starts.
    // To create connections after the container has started, use
    // container::connect().
    // See @ref multithreaded_client_flow_control.cpp for an example.
    void on_container_start(proton::container& cont) override
    {
      log_debug("on_container_start");
      m_connection = cont.connect(m_url);
    }

    void on_connection_open(proton::connection& conn) override
    {
      log_debug("on_connection_open");

      conn.open_sender(m_senderAddress);

      // conn.open_sender(address_);
      // conn.open_receiver(address_);

      // proton::receiver_options receiverOpts {};
      // proton::source_options sourceOpts {};
      // sourceOpts.capabilities(std::vector<proton::symbol> { "topic" });
      // receiverOpts.source(sourceOpts);
      // conn.open_receiver(address_, receiverOpts);

      // proton::sender_options senderOpts {};
      // proton::target_options targetOpts {};

      // targetOpts.capabilities(std::vector<proton::symbol> { "topic" });
      // senderOpts.target(targetOpts);

      // conn.open_sender(address_);

      // proton::sender_options senderOpts {};
      // proton::target_options targetOpts {};

      // targetOpts.capabilities(std::vector<proton::symbol> { "topic" });
      // senderOpts.target(targetOpts);

      //conn.open_sender(address_, senderOpts);
    }

    void on_connection_close(proton::connection&) override
    {
      log_debug("on_connection_close");
    }

    void on_container_stop(proton::container&) override
    {
      log_debug("on_container_stop");
    }

    void on_sender_open(proton::sender& sender) override
    {
      // m_sender and work_queue_ must be set atomically
      log_debug("Open sender for target address: %s", sender.target().address().c_str());
      std::lock_guard<std::mutex> l(lock_);
      m_sender = sender;
      work_queue_ = &sender.work_queue();
      sender_ready_.notify_all();
    }

    void on_receiver_open(proton::receiver& receiver) override
    {
      log_debug("Open receiver for target address: %s", receiver.source().address().c_str());
      m_receiver = receiver;
    }

    void on_message(proton::delivery& dlv, proton::message& msg) override
    {
      std::lock_guard<std::mutex> l(lock_);
      messages_.push(msg);
      messages_ready_.notify_all();
    }

    void on_transport_error(proton::transport& t) override
    {
      // std::string err = t.error().what();
      // std::cout << "on_transport_error " << err << std::endl;
      //log_error("on_transport_error: %s", t.error().what().c_str());
      OUT(std::cerr << "on_transport_error: " << t.error().what() << std::endl);
    }

    void on_error(const proton::error_condition& e) override
    {
      OUT(std::cerr << "unexpected error: " << e << std::endl);
      //exit(1);
    }
  };

} // namespace fty::messagebus::amqp
