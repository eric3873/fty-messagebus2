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

// #include <proton/connection.hpp>
// #include <proton/container.hpp>
// #include <proton/delivery.hpp>
// #include <proton/message.hpp>
// #include <proton/messaging_handler.hpp>
// #include <proton/receiver_options.hpp>
// #include <proton/source_options.hpp>
// #include <proton/tracker.hpp>

#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/sender.hpp>
#include <proton/sender_options.hpp>
#include <proton/target.hpp>
#include <proton/target_options.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>

#include <future>
#include <iostream>
#include <vector>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  //static auto constexpr AMQP_CORREL_ID = "JMSCorrelationID";

  using proton::receiver_options;
  using proton::source_options;

  class Requester : public proton::messaging_handler
  {
  private:
    std::string m_url;
    proton::message m_request;
    // Synchronisation
    std::promise<proton::message> m_promise;
    std::future<proton::message> m_future;
    std::mutex m_lock;
    // sender and receiver
    proton::sender m_sender;
    proton::receiver m_receiver;

    void set_filter(proton::source_options& opts, const std::string& selector_str)
    {
      proton::source::filter_map map;
      proton::symbol filter_key("selector");
      proton::value filter_value;
      // The value is a specific AMQP "described type": binary string with symbolic descriptor
      proton::codec::encoder enc(filter_value);
      enc << proton::codec::start::described()
          << proton::symbol("apache.org:selector-filter:string")
          << selector_str
          << proton::codec::finish();
      // In our case, the map has this one element
      map.put(filter_key, filter_value);
      opts.filters(map);
    }

  public:
    Requester(const std::string& url, const proton::message& message)
      : m_url(url)
      , m_request(message)
    {
      m_future = m_promise.get_future();
    }

    void on_container_start(proton::container& con) override
    {
      logDebug("on_container_start");
      proton::connection conn = con.connect(m_url);

      m_sender = conn.open_sender(m_request.to());
      // Create a receiver requesting a dynamically created queue
      // for the message source.
      //receiver_options opts = receiver_options().source(source_options().dynamic(true));
      //m_receiver = m_sender.connection().open_receiver("", opts);
      proton::source_options opts;
      if (!m_request.correlation_id().empty())
      {
        std::ostringstream correlIdFilter;
        correlIdFilter << AMQP_CORREL_ID;
        correlIdFilter << "='";
        correlIdFilter << proton::to_string(m_request.correlation_id());
        correlIdFilter << "'";
        logDebug("CorrelId filter: {}", correlIdFilter.str().c_str());
        set_filter(opts, correlIdFilter.str());
      }
      m_receiver = m_sender.connection().open_receiver("queue://" + m_request.reply_to(), proton::receiver_options().source(opts));
    }

    void send_request()
    {
      logDebug("send_request");
      // TODO see where to set this.
      //m_request.reply_to(m_receiver.source().address());
      m_sender.send(m_request);
    }

    bool tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout)
    {
      logDebug("Checking answer for {} second(s), please wait...", timeout);

      bool messageArrived = false;
      if (m_future.wait_for(std::chrono::seconds(timeout)) != std::future_status::timeout)
      {
        *resp = std::move(m_future.get());
        messageArrived = true;
      }
      cancel();
      return messageArrived;
    }

    void on_sender_open(proton::sender& snd) override
    {
      logDebug("Open sender for target address: {}", snd.target().address());
    }

    void on_receiver_open(proton::receiver& receiver) override
    {
      logDebug("Open receiver for target address: {}", receiver.source().address());
      send_request();
    }

    void cancel()
    {
      std::lock_guard<std::mutex> l(m_lock);
      logDebug("Canceling");
      if (m_receiver)
      {
        m_receiver.connection().close();
      }
      if (m_sender)
      {
        m_sender.connection().close();
      }
      logDebug("Canceled");
    }

    void on_message(proton::delivery& delivery, proton::message& msg) override
    {
      std::lock_guard<std::mutex> l(m_lock);
      delivery.accept();
      m_promise.set_value(msg);
    }
  };

} // namespace fty::messagebus::amqp
