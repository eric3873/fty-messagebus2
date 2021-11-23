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

#include "AmqpClient.h"

#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  using namespace fty::messagebus;
  using MessageListener = fty::messagebus::MessageListener;

  static auto constexpr TIMEOUT = std::chrono::seconds(1);
  static auto constexpr AMQP_CORREL_ID = "JMSCorrelationID";

  AmqpClient::AmqpClient(const std::string& url)
    : m_url(url)
  {
    m_connectFuture = m_connectPromise.get_future();
  }

  AmqpClient::~AmqpClient()
  {
    close();
  }

  void AmqpClient::on_container_start(proton::container& container)
  {
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

  void AmqpClient::on_connection_open(proton::connection& connection)
  {
    logDebug("Connected on url: {}", m_url);
    m_connection = connection;
    m_connectPromise.set_value(ComState::COM_STATE_OK);
  }

  void AmqpClient::on_sender_open(proton::sender& sender)
  {
    logDebug("Sending message ...");
    sender.send(m_message);
    sender.connection().close();
    m_message = {};
    m_promiseSender.set_value();
    logDebug("Message sent");
  }

  void AmqpClient::on_receiver_open(proton::receiver& receiver)
  {
    logDebug("Waiting any message on target address: {}", receiver.source().address());
    // Record receiver to have the possibility to unreceive it (i.e. close it)
    m_receiver = receiver;
    m_promiseReceiver.set_value();
  }

  void AmqpClient::on_error(const proton::error_condition& error)
  {
    logError("Protocol error: {}", error.what());
  }

  ComState AmqpClient::connected()
  {
    if ((m_communicationState == ComState::COM_STATE_UNKNOWN) && (m_connectFuture.wait_for(TIMEOUT) != std::future_status::timeout))
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
    return m_communicationState;
  }

  DeliveryState AmqpClient::send(const proton::message& msg)
  {
    auto deliveryState = DeliveryState::DELIVERY_STATE_REJECTED;
    if (connected() == ComState::COM_STATE_OK)
    {
      m_promiseSender = std::promise<void>();
      logDebug("Sending message to {} ...", msg.to());
      m_message = msg;

      m_connection.work_queue().add([=]() {
        m_connection.open_sender(msg.to());
      });

      // Wait the to know if the message has been sent or not
      if (m_promiseSender.get_future().wait_for(TIMEOUT) != std::future_status::timeout)
      {
        deliveryState = DeliveryState::DELIVERY_STATE_ACCEPTED;
      }
    }
    return deliveryState;
  }

  DeliveryState AmqpClient::receive(const std::string& address, const std::string& filter, MessageListener messageListener)
  {
    auto deliveryState = DeliveryState::DELIVERY_STATE_REJECTED;
    if (connected() == ComState::COM_STATE_OK)
    {
      logDebug("Set receiver to wait message(s) from {} ...", address);
      m_promiseReceiver = std::promise<void>();

      auto futureReceiver = m_promiseReceiver.get_future();
      proton::receiver_options receiverOptions;
      if (!filter.empty())
      {
        // Receiver with filtering, so reply, the filtering for this implementation is only on correlationId
        std::ostringstream correlIdFilter;
        correlIdFilter << AMQP_CORREL_ID;
        correlIdFilter << "='";
        correlIdFilter << filter;
        correlIdFilter << "'";
        logDebug("CorrelId filter: {}", correlIdFilter.str());
        //receiverOptions = getReceiverOptions(correlIdFilter.str());
        setSubscriptions(filter, messageListener);
      }
      else
      {
        setSubscriptions(address, messageListener);
      }

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

  bool AmqpClient::tryConsumeMessageFor(std::shared_ptr<proton::message> resp, int timeout)
  {
    logDebug("Checking answer for {} second(s)...", timeout);

    m_promiseSyncRequest = std::promise<proton::message>();

    bool messageArrived = false;
    auto futureSynRequest = m_promiseSyncRequest.get_future();
    if (futureSynRequest.wait_for(std::chrono::seconds(timeout)) != std::future_status::timeout)
    {
      try
      {
        *resp = std::move(futureSynRequest.get());
        messageArrived = true;
      }
      catch (const std::future_error& e)
      {
        logError("Caught a future_error {}", e.what());
      }
    }
    return messageArrived;
  }

  void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
  {
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Message arrived {}", proton::to_string(msg));
    delivery.accept();
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection)
    {
      // Asynchronous reply or any subscription
      if (!msg.correlation_id().empty() && msg.reply_to().empty())
      {
        // A reply message
        if (auto it{m_subscriptions.find(proton::to_string(msg.correlation_id()))}; it != m_subscriptions.end())
        {
          logDebug("Asynchronous mode");
          m_connection.work_queue().add(proton::make_work(it->second, amqpMsg));
        }
        else
        {
          // Synchronous reply
          logDebug("Synchronous mode");
          m_promiseSyncRequest.set_value(msg);
        }
      }
      else
      {
        if (auto it{m_subscriptions.find(msg.address())}; it != m_subscriptions.end())
        {
          m_connection.work_queue().add(proton::make_work(it->second, amqpMsg));
        }
        else
        {
          logWarn("Message skipped for {}", msg.address());
        }
      }

      // m_connection.work_queue().add(proton::make_work(m_messageListener, amqpMsg));
      if (!msg.correlation_id().empty() && msg.reply_to().empty())
      {
        //unreceive();
      }
    }
    else
    {
      // Connection object not set
      logError("Nothing to do connection object not set");
    }
  }

  void AmqpClient::setSubscriptions(const std::string& address, MessageListener messageListener)
  {
    if (auto it{m_subscriptions.find(address)}; it == m_subscriptions.end() && messageListener)
    {
      auto ret = m_subscriptions.emplace(address, messageListener);
      logTrace("Subscriptions emplaced: {} {}", address, ret.second ? "true" : "false");
    }
    else
    {
      logWarn("Set subscriptions skipped");
    }
  }

  proton::receiver_options AmqpClient::getReceiverOptions(const std::string& selector) const
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

  void AmqpClient::unreceive()
  {
    if (m_receiver)
    {
      logDebug("Closing receiver");
      m_receiver.close();
    }
  }

  void AmqpClient::close()
  {
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Closing...");
    if (m_connection)
    {
      logDebug("Closing connection");
      m_connection.close();
    }
    logDebug("Closed");
  }

} // namespace fty::messagebus::amqp
