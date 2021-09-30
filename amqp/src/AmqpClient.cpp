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

#include "AmqpClient.h"

#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  static auto constexpr AMQP_CORREL_ID = "JMSCorrelationID";
  using MessageListener = fty::messagebus::MessageListener;
  using fty::messagebus::Message;

  AmqpClient::AmqpClient(const std::string& url, const std::string& address, MessageListener messageListener, const std::string& filter)
    : m_url(url)
    , m_address(address)
    , m_messageListener(std::move(messageListener))
    , m_filter(filter)
  {
    m_future = m_promise.get_future();
  }

  void AmqpClient::on_container_start(proton::container& container)
  {
    logDebug("AmqpClient on_container_start");
    try
    {
      container.connect(m_url);
    }
    catch (std::exception& e)
    {
      log_error("Exception {}", e.what());
    }
  }

  void AmqpClient::on_connection_open(proton::connection& connection)
  {
    logDebug("on_connection_open for target address: {}", m_address);

    //if (m_messageListener)
    if (!m_filter.empty())
    {
      proton::source_options opts;
      // if (!m_filter.empty())
      // {
        std::ostringstream correlIdFilter;
        correlIdFilter << AMQP_CORREL_ID;
        correlIdFilter << "='";
        correlIdFilter << m_filter;
        correlIdFilter << "'";
        logDebug("CorrelId filter: {}", correlIdFilter.str());
        opts = setFilter(correlIdFilter.str());
      //}
      m_receiver = connection.open_receiver(m_address, proton::receiver_options().source(opts));
    }
    else if (m_messageListener)
    {
      m_receiver = connection.open_receiver(m_address);
    }
    else
    {
      connection.open_sender(m_address);
    }
  }

  void AmqpClient::on_sender_open(proton::sender& sender)
  {
    logDebug("on_sender_open for target address: {}", sender.source().address());
    std::unique_lock<std::mutex> l(m_lock);
    m_sender = sender;
    m_cvAmqpClientReady.notify_all();
  }

  void AmqpClient::on_receiver_open(proton::receiver& receiver)
  {
    logDebug("on_receiver_open for target address: {}", receiver.source().address());
  }

  void AmqpClient::sendMsg(const proton::message& msg)
  {
    logDebug("Init sender waiting...");
    std::unique_lock<std::mutex> l(m_lock);
    m_cvAmqpClientReady.wait(l);
    logDebug("sender ready on {}", msg.to());

    m_sender.work_queue().add([=]() {
      auto tracker = m_sender.send(msg);
      logDebug("Msg sent {}", proton::to_string(tracker.state()));
      m_sender.connection().close();
    });
  }

  bool AmqpClient::tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout)
  {
    logDebug("Checking answer for {} second(s)...", timeout);

    bool messageArrived = false;
    if (m_future.wait_for(std::chrono::seconds(timeout)) != std::future_status::timeout)
    {
      *resp = std::move(m_future.get());
      messageArrived = true;
    }
    close();
    return messageArrived;
  }

  void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
  {
    std::lock_guard<std::mutex> l(m_lock);
    logDebug("Message arrived {}", proton::to_string(msg));
    delivery.accept();
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));
    m_receiver.work_queue().add(proton::make_work(m_messageListener, amqpMsg));
  }

  proton::source_options AmqpClient::setFilter(const std::string& selector)
  {
    proton::source_options opts;
    proton::source::filter_map map;
    proton::symbol filterKey("selector");
    proton::value filterValue;
    // The value is a specific AMQP "described type": binary string with symbolic descriptor
    proton::codec::encoder enc(filterValue);
    enc << proton::codec::start::described()
        << proton::symbol("apache.org:selector-filter:string")
        << selector
        << proton::codec::finish();
    // In our case, the map has this one element
    map.put(filterKey, filterValue);
    opts.filters(map);

    return opts;
  }

  void AmqpClient::close()
  {
    std::lock_guard<std::mutex> l(m_lock);
    logDebug("Closing sender for {}", m_address);
    if (m_sender)
    {
      m_sender.connection().close();
    }
    if (m_receiver)
    {
      m_receiver.connection().close();
    }
    logDebug("Closed");
  }

} // namespace fty::messagebus::amqp
