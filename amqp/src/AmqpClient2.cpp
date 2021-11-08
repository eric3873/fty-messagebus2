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

#include "AmqpClient2.h"

#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  using namespace fty::messagebus;
  using MessageListener = fty::messagebus::MessageListener;

  static auto constexpr TIMEOUT = std::chrono::seconds(5);
  static auto constexpr AMQP_CORREL_ID = "JMSCorrelationID";

  AmqpClient2::AmqpClient2(const std::string& url, const std::string& address, const std::string& filter, MessageListener messageListener)
    : m_url(url)
    , m_address(address)
    , m_filter(filter)
    , m_messageListener(std::move(messageListener))
  {
    m_future = m_promise.get_future();
    m_connectFuture = m_connectPromise.get_future();
  }

  void AmqpClient2::on_container_start(proton::container& container)
  {
    logDebug("AmqpClient2 on_container_start");
    try
    {
      container.connect(m_url);
    }
    catch (std::exception& e)
    {
      log_error("Exception {}", e.what());
      m_connectPromise.set_value(ComState::COM_STATE_CONNECT_FAILED);
    }
  }

  void AmqpClient2::on_connection_open(proton::connection& connection)
  {
    logDebug("Connected on url: {}", m_url);
    m_connection = connection;
    m_connectPromise.set_value(ComState::COM_STATE_OK);

    // if (!m_filter.empty())
    // {
    //   // Receiver with filtering, so reply, the filtering for this implementation is only on correlationId
    //   proton::source_options opts;
    //   std::ostringstream correlIdFilter;
    //   correlIdFilter << AMQP_CORREL_ID;
    //   correlIdFilter << "='";
    //   correlIdFilter << m_filter;
    //   correlIdFilter << "'";
    //   logDebug("CorrelId filter: {}", correlIdFilter.str());
    //   opts = setFilter(correlIdFilter.str());

    //   m_receiver = connection.open_receiver(m_address, proton::receiver_options().source(opts));
    // }
    // else if (m_messageListener)
    // {
    //   // Receiver without filtering, so request or subscription
    //   m_receiver = connection.open_receiver(m_address);
    // }
    // else
    // {
    //   // Sender only
    //   connection.open_sender(m_address);
    // }
  }

  void AmqpClient2::on_sender_open(proton::sender& sender)
  {
    logDebug("On sender open for target address: {}", sender.source().address());
    auto tracker = sender.send(m_message);
    sender.connection().close();
    m_message = {};
    m_promiseSender.set_value();
    // // std::unique_lock<std::mutex> l(m_lock);
    // // m_senderReady.notify_all();
    logDebug("Msg sent {}", proton::to_string(tracker.state()));
  }

  // void AmqpClient2::on_sendable(proton::sender& sender)
  // {
  //   auto tracker = sender.send(m_message);
  //   sender.connection().close();
  //   m_message = {};
  //   //m_promiseSender.set_value();
  //   // std::unique_lock<std::mutex> l(m_lock);
  //   // m_senderReady.notify_all();
  //   logDebug("Msg sent {}", proton::to_string(tracker.state()));
  // }

  void AmqpClient2::on_receiver_open(proton::receiver& receiver)
  {
    logDebug("On receiver open for target address: {}", receiver.source().address());
    m_promiseReceiver.set_value();
  }

  ComState AmqpClient2::connected()
  {
    logDebug("in connected");
    if (m_communicationState == ComState::COM_STATE_UNKNOWN)
    {
      if (m_connectFuture.wait_for(TIMEOUT) != std::future_status::timeout)
      {
        try
        {
          m_communicationState = m_connectFuture.get();
        }
        catch (const std::future_error& e)
        {
          logError("Caught a future_error {}", e.what());
        }
      }
    }
    return m_communicationState;
  }

  DeliveryState AmqpClient2::send(const proton::message& msg)
  {
    auto deliveryState = DeliveryState::DELIVERY_STATE_REJECTED;
    if (connected() == ComState::COM_STATE_OK)
    {
      m_promiseSender = std::promise<void>();
      logDebug("Sending message to {} ...", msg.to());
      m_message = msg;

      m_connection.work_queue().add([=]() {
        //proton::make_work(m_connection.open_sender(msg.to()));
        m_connection.open_sender(msg.to());
        //m_message = msg;
      });
      //m_promiseSender = std::promise<void>();
      //std::this_thread::sleep_for(std::chrono::milliseconds(100));
      // std::unique_lock<std::mutex> l(m_lock);
      // m_senderReady.wait(l);

      // Wait the to know if the message has been sent or not
      if (m_promiseSender.get_future().wait_for(TIMEOUT) != std::future_status::timeout)
      {
        deliveryState =  DeliveryState::DELIVERY_STATE_ACCEPTED;
      }
    }
    return deliveryState;
  }

  DeliveryState AmqpClient2::receive(const std::string& address, const std::string& filter, MessageListener messageListener)
  {
    auto deliveryState = DeliveryState::DELIVERY_STATE_REJECTED;
    if (connected() == ComState::COM_STATE_OK)
    {
      m_promiseReceiver = std::promise<void>();
      auto futureReceiver = m_promiseReceiver.get_future();
      logDebug("Set receiver to wait message(s) from {} ...", address);
      proton::receiver_options receiverOptions;
      m_messageListener = messageListener;


      if (!filter.empty())
      {
        // Receiver with filtering, so reply, the filtering for this implementation is only on correlationId
        std::ostringstream correlIdFilter;
        correlIdFilter << AMQP_CORREL_ID;
        correlIdFilter << "='";
        correlIdFilter << filter;
        correlIdFilter << "'";
        logDebug("CorrelId filter: {}", correlIdFilter.str());
        receiverOptions = getReceiverOptions(correlIdFilter.str());
      }

      //m_receiverListener.emplace(std::make_pair(address, messageListener));
      //m_receiverListener.emplace(address, messageListener);
      //m_receiverListener.try_emplace(address, messageListener);

      m_connection.work_queue().add([=]() {
        m_connection.open_receiver(address, receiverOptions);
      });

      if (futureReceiver.wait_for(TIMEOUT) != std::future_status::timeout)
      {
        deliveryState = DeliveryState::DELIVERY_STATE_ACCEPTED;
      }
    }

    return deliveryState;
  }

  bool AmqpClient2::tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout)
  {
    logDebug("Checking answer for {} second(s)...", timeout);

    bool messageArrived = false;
    if (m_future.wait_for(std::chrono::seconds(timeout)) != std::future_status::timeout)
    {
      try
      {
        *resp = std::move(m_future.get());
        messageArrived = true;
      }
      catch (const std::future_error& e)
      {
        logError("Caught a future_error {}", e.what());
      }
    }
    close();
    return messageArrived;
  }

  void AmqpClient2::on_message(proton::delivery& delivery, proton::message& msg)
  {
    std::lock_guard<std::mutex> l(m_lock);
    logDebug("Message arrived {}", proton::to_string(msg));
    delivery.accept();
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    //auto iterator = m_receiverListener.find(msg.address());
    //if (m_connection && iterator != m_receiverListener.end())
    if (m_connection && m_messageListener)
    {
      // Asynchronous reply or any subscription
      logDebug("Asynchronous reply");
      //m_connection.work_queue().add(proton::make_work(iterator->second, amqpMsg));
      m_connection.work_queue().add(proton::make_work(m_messageListener, amqpMsg));
    }
    else
    {
      // Synchronous reply
      logDebug("Synchronous reply");
      m_promise.set_value(msg);
    }
  }

  proton::receiver_options AmqpClient2::getReceiverOptions(const std::string& selector) const
  {
    proton::receiver_options receiverOptions;
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
    receiverOptions.source(opts);

    return receiverOptions;
  }

  void AmqpClient2::close()
  {
    std::lock_guard<std::mutex> l(m_lock);
    logDebug("Closing...");
    if (m_sender)
    {
      logDebug("Closing sender for {}", m_address);
      m_sender.connection().close();
    }
    if (m_receiver)
    {
      logDebug("Closing receiver for {}", m_address);
      m_receiver.connection().close();
    }
    logDebug("Closed");
  }

} // namespace fty::messagebus::amqp
