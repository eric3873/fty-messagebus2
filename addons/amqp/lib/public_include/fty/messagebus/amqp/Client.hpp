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

#include <fty/messagebus/IMessageBus.hpp>

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/sender.hpp>
#include <proton/work_queue.hpp>

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
#define OUT(x) do { std::lock_guard<std::mutex> l(out_lock); x; } while (false)

// Handler for a single thread-safe sending and receiving connection.
class client : public proton::messaging_handler {
    // Invariant
    const std::string url_;
    const std::string address_;

    // Only used in proton handler thread
    proton::sender sender_;

    // Shared by proton and user threads, protected by lock_
    std::mutex lock_;
    proton::work_queue *work_queue_;
    std::condition_variable sender_ready_;
    std::queue<proton::message> messages_;
    std::condition_variable messages_ready_;

  public:
    client(const std::string& url, const std::string& address) : url_(url), address_(address), work_queue_(0) {}

    // Thread safe
    void send(const proton::message& msg) {
        // Use [=] to copy the message, we cannot pass it by reference since it
        // will be used in another thread.
        std::cout << "sending " << std::endl;

        work_queue()->add([=]() { sender_.send(msg); });
    }

    // Thread safe
    proton::message receive() {
        std::unique_lock<std::mutex> l(lock_);
        while (messages_.empty()) messages_ready_.wait(l);
        auto msg = std::move(messages_.front());
        messages_.pop();
        return msg;
    }

    // Thread safe
    void close() {
        work_queue()->add([=]() { sender_.connection().close(); });
    }

  private:

    proton::work_queue* work_queue() {
        // Wait till work_queue_ and sender_ are initialized.
        std::unique_lock<std::mutex> l(lock_);
        while (!work_queue_) sender_ready_.wait(l);
        return work_queue_;
    }

    // == messaging_handler overrides, only called in proton handler thread

    // Note: this example creates a connection when the container starts.
    // To create connections after the container has started, use
    // container::connect().
    // See @ref multithreaded_client_flow_control.cpp for an example.
    void on_container_start(proton::container& cont) override {
        std::cout << "on_container_start " << std::endl;
        cont.connect(url_);
    }

    void on_connection_open(proton::connection& conn) override {
        std::cout << "on_connection_open " << std::endl;

        conn.open_sender(address_);
        conn.open_receiver(address_);
    }

    void on_sender_open(proton::sender& s) override {
        // sender_ and work_queue_ must be set atomically
        std::cout << "on_sender_open " << std::endl;

        std::lock_guard<std::mutex> l(lock_);
        sender_ = s;
        work_queue_ = &s.work_queue();
        sender_ready_.notify_all();
    }

    void on_message(proton::delivery& dlv, proton::message& msg) override {
        std::lock_guard<std::mutex> l(lock_);
        messages_.push(msg);
        messages_ready_.notify_all();
    }

    void on_error(const proton::error_condition& e) override {
        OUT(std::cerr << "unexpected error: " << e << std::endl);
        exit(1);
    }
};

} // namespace fty::messagebus::amqp
