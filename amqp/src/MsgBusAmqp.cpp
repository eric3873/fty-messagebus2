/*  =========================================================================
    MsgBusAmqp.cpp - class description

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

#include "MsgBusAmqp.h"
#include "MsgBusAmqpUtils.h"

#include <fty/messagebus/utils.h>
#include <fty_log.h>

namespace
{
  // TODO implement it
  bool isServiceAvailable()
  {
    return true;
  }
} // namespace

namespace fty::messagebus::amqp
{
  static auto constexpr AMQP_TOPIC_PREFIX = "topic://";
  static auto constexpr AMQP_QUEUE_PREFIX = "queue://";

  using namespace fty::messagebus;

  using proton::receiver_options;
  using proton::source_options;

  MsgBusAmqp::~MsgBusAmqp()
  {
    // Cleaning amqp ressources
    if (isServiceAvailable())
    {
      logDebug("Cleaning Amqp ressources for: {}", m_clientName);
      try
      {
        for (const auto& [key, receiv] : m_subScriptions)
        {
          logDebug("Cleaning: {}...", key);
          //receiv.cancel();
        }
        logDebug("{} cleaned", m_clientName);
      }
      catch (const std::exception& e)
      {
        logError("Exception: {}", e.what());
      }
    }
  }

  fty::Expected<void> MsgBusAmqp::connect()
  {

    logDebug("Connecting to {} ...", m_endpoint);
    try
    {
    }
    catch (const std::exception& e)
    {
      logError("unexpected error: {}", e.what());
      return fty::unexpected(COM_STATE_CONNECT_FAILED);
    }

    return {};
  }

  fty::Expected<void> MsgBusAmqp::publish(const std::string& address, const Message& /*message*/)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logDebug("Publishing on: {}", address);

    // TODO insert real impl
    if (true)
    {
      //message rejected
      logDebug("Message rejected");
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Message published (Accepted)");

    return {};
  }

  fty::Expected<void> MsgBusAmqp::subscribe(const std::string& address, MessageListener messageListener)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logDebug("Try to subscribing on: {}", address);

    ReceiverPointer receiver = std::make_shared<Receiver>(m_endpoint, address, messageListener);
    std::thread thrd([=]() {
      proton::container(*receiver).run();
    });
    m_subScriptions.emplace(std::make_pair(address, receiver));
    thrd.detach();

    // logDebug("Subscribed ({})", DELIVERY_STATE_REJECTED);
    // return fty::unexpected(DELIVERY_STATE_REJECTED);

    logDebug("Subscribed (Accepted)");

    return {};
  }

  fty::Expected<void> MsgBusAmqp::unsubscribe(const std::string& address)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logTrace("{} - unsubscribed on: '{}'", m_clientName, address);

    // TODO insert real impl
    if (true)
    {
      logDebug("Unsubscribed ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    return {};
  }

  fty::Expected<void> MsgBusAmqp::receive(const std::string& queue, MessageListener messageListener, const std::string& filter)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    std::string amqpQueue = /*AMQP_QUEUE_PREFIX + */queue;
    logDebug("Waiting to receive msg from: {}", amqpQueue);

    // TODO see filter
    ReceiverPointer receiver = std::make_shared<Receiver>(m_endpoint, amqpQueue, messageListener, filter);
    std::thread thrd([=]() {
      proton::container(*receiver).run();
    });
    m_subScriptions.emplace(std::make_pair(amqpQueue, receiver));
    thrd.detach();

    logDebug("Waiting to receive msg from: {} Accepted", amqpQueue);
    return {};
  }

  fty::Expected<void> MsgBusAmqp::receive(const std::string& queue, MessageListener messageListener)
  {
    return receive(queue, messageListener, {});
  }

  fty::Expected<void> MsgBusAmqp::sendRequest(const std::string& requestQueue, const Message& message)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    logDebug("Request sent to {}", requestQueue);

    std::string amqpQueue = /*AMQP_QUEUE_PREFIX +*/ requestQueue;
    proton::message msgToSend = getAmqpMessageFromMsgBusAmqpMessage(message);
    // TODO remove from here
    msgToSend.to(amqpQueue);

    std::string replyTo = "<none>";
    if (message.needReply())
    {
      msgToSend.reply_to(/*AMQP_QUEUE_PREFIX + */msgToSend.reply_to());
    }

    //logDebug("Sending request payload: '{}' to: {} and wait message on reply queue {}", message.userData(), requestQueue, replyTo);

    Sender sender = Sender(m_endpoint, amqpQueue);
    std::thread thrd([&]() {
      proton::container(sender).run();
    });
    sender.sendMsg(msgToSend);
    thrd.join();

    if (false)
    {
      logDebug("Request sent ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Request sent (Accepted)");
    return {};
  }

  fty::Expected<void> MsgBusAmqp::sendRequest(const std::string& requestQueue, const Message& message, MessageListener messageListener)
  {
    auto delivState = receive(requestQueue, messageListener);
    if (!delivState)
    {
      return fty::unexpected(delivState.error());
    }
    return sendRequest(requestQueue, message);
  }

  fty::Expected<void> MsgBusAmqp::sendReply(const std::string& /*replyQueue*/, const Message& message)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
    }

    // Adding all meta data inside mqtt properties
    proton::message msgToSend = getAmqpMessageFromMsgBusAmqpMessage(message);

    //logDebug("Sending reply payload: '{}' to: {}", message.userData(), msgToSend.to());

    Sender sender = Sender(m_endpoint, msgToSend.to());
    std::thread thrd([&]() {
      proton::container(sender).run();
    });
    sender.sendMsg(msgToSend);
    thrd.join();

    if (false)
    {
      logDebug("Reply sent ({})", DELIVERY_STATE_REJECTED);
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    logDebug("Reply sent (Accepted)");

    return {};
  }

  fty::Expected<Message> MsgBusAmqp::request(const std::string& requestQueue, const Message& message, int receiveTimeOut)
  {
    try
    {
      if (!isServiceAvailable())
      {
        logDebug("Service not available");
        return fty::unexpected(DELIVERY_STATE_UNAVAILABLE);
      }

      std::string amqpQueue = /*AMQP_QUEUE_PREFIX + */requestQueue;
      std::string replyTo = /*AMQP_QUEUE_PREFIX + */fty::messagebus::amqp::getReplyQueue(message);

      auto protonMsg = getAmqpMessageFromMsgBusAmqpMessage(message);
      // TODO remove from here
      protonMsg.to(amqpQueue);

      Requester requester(m_endpoint, protonMsg);
      std::thread thrd([&]() {
        proton::container(requester).run();
      });
      thrd.detach();

      MessagePointer response = std::make_shared<proton::message>();
      bool messageArrived = requester.tryConsumeMessageFor(response, receiveTimeOut);
      if (!messageArrived)
      {
        logDebug("No message arrive in time!");
        return fty::unexpected(DELIVERY_STATE_TIMEOUT);
      }

      logDebug("Message arrived ({})", proton::to_string(*response));
      return Message{getMetaDataFromAmqpProperties(*response), response->body().empty() ? std::string{} : proton::to_string(response->body())};
    }
    catch (std::exception& e)
    {
      return fty::unexpected(e.what());
    }
  }

} // namespace fty::messagebus::amqp
