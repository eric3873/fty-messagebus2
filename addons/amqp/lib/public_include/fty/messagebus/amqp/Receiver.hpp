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

#include "fty/messagebus/amqp/MsgBusAmqpUtils.hpp"
#include <fty/messagebus/IMessageBus.hpp>
#include <fty/messagebus/amqp/MsgBusAmqpMessage.hpp>

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
  static auto constexpr AMQP_CORREL_ID = "JMSCorrelationID";
  using MessageListener = fty::messagebus::MessageListener<AmqpMessage>;

  class Receiver : public proton::messaging_handler
  {
  private:
    std::string m_url;
    std::string m_address;
    MessageListener m_messageListener;
    std::string m_filter;

    std::mutex m_lock;
    // receiver
    proton::receiver m_receiver;
    //std::unique_ptr<proton::work_queue> p_work_queue;
    //proton::work_queue* p_work_queue;

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
    Receiver(const std::string& url, const std::string& address, MessageListener messageListener, const std::string& filter = "")
      : m_url(url)
      , m_address(address)
      , m_messageListener(std::move(messageListener))
      , m_filter(filter)
    {
    }

    ~Receiver()
    {
      cancel();
    }

    void on_container_start(proton::container& con) override
    {
      log_debug("Receiver on_container_start");
      try
      {
        proton::connection conn = con.connect(m_url);
        proton::source_options opts;
        if (!m_filter.empty())
        {
          std::ostringstream correlIdFilter;
          correlIdFilter << AMQP_CORREL_ID;
          correlIdFilter << "='";
          correlIdFilter << m_filter;
          correlIdFilter << "'";
          log_debug("CorrelId filter: %s", correlIdFilter.str().c_str());
          set_filter(opts, correlIdFilter.str());
        }
        m_receiver = conn.open_receiver(m_address, proton::receiver_options().source(opts));
      }
      catch (std::exception& e)
      {
        log_error("Exception %s", e.what());
      }
    }

    void on_receiver_open(proton::receiver& receiver) override
    {
      log_debug("Receiver on_receiver_open for target address: %s", receiver.source().address().c_str());
      //p_work_queue.reset(&receiver.work_queue());
      //p_work_queue = &receiver.work_queue();
    }

    void cancel()
    {
      std::lock_guard<std::mutex> l(m_lock);
      log_debug("Cancel for %s", m_address.c_str());
      if (m_receiver)
      {
        m_receiver.connection().close();
      }
      log_debug("Canceled");
    }

    void on_message(proton::delivery& delivery, proton::message& msg) override
    {
      std::lock_guard<std::mutex> l(m_lock);
      log_debug("Message arrived on: %s", m_address.c_str());
      //m_work_queue.add(make_work(&Queue::unsubscribe, s->queue_, s));
      //p_work_queue->add([=]() { this->print(msg);});
      AmqpMessage amqpMsg(getMetaDataFromAmqpProperties(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));
      //p_work_queue->add(proton::make_work(m_messageListener, amqpMsg));
      m_receiver.work_queue().add(proton::make_work(m_messageListener, amqpMsg));
      delivery.accept();
    }
  };

} // namespace fty::messagebus::amqp
