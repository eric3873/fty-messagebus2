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

#include "MsgBusAmqp.h"
#include "MsgBusAmqpUtils.h"

#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/utils.h>
#include <fty_log.h>

namespace fty::messagebus::amqp
{
  using namespace fty::messagebus;
  using proton::receiver_options;
  using proton::source_options;

  MsgBusAmqp::~MsgBusAmqp()
  {
    // Cleaning amqp ressources
    if (isServiceAvailable())
    {
      logDebug("Cleaning Amqp ressources for: {}", m_clientName);

      for (const auto& [key, receiver] : m_subScriptions)
      {
        logDebug("Cleaning: {}...", key);
        receiver->close();
      }
      logDebug("{} cleaned", m_clientName);
    }
  }

  fty::Expected<void> MsgBusAmqp::connect()
  {
    logDebug("Connecting to {} ...", m_endpoint);
    try
    {
      m_amqpClient = std::make_shared<AmqpClient>(m_endpoint);
      std::thread thrdSender([=]() {
        proton::container(*m_amqpClient).run();
      });
      thrdSender.detach();

      if (m_amqpClient->connected() != ComState::COM_STATE_OK)
      {
        return fty::unexpected(to_string(m_amqpClient->connected()));
      }
    }
    catch (const std::exception& e)
    {
      logError("Unexpected error: {}", e.what());
      return fty::unexpected(to_string(ComState::COM_STATE_CONNECT_FAILED));
    }
    return {};
  }

  bool MsgBusAmqp::isServiceAvailable()
  {
    return (m_amqpClient && (m_amqpClient->connected() == ComState::COM_STATE_OK)) ? true : false;
  }

  fty::Expected<void> MsgBusAmqp::receive(const std::string& address, MessageListener messageListener, const std::string& filter)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    auto receiver = std::make_shared<AmqpClient>(m_endpoint);
    std::thread thrd([=]() {
      proton::container(*receiver).run();
    });
    auto received = receiver->receive(address, filter, messageListener);
    m_subScriptions.emplace(address, receiver);
    logDebug("m_subScriptions nb: {}, {}", m_subScriptions.size(), address);
    thrd.detach();

    if (received != DeliveryState::DELIVERY_STATE_ACCEPTED)
    {
      logError("Message receive (Rejected)");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }

    logDebug("Waiting to receive msg from: {} Accepted", address);
    return {};
  }

  fty::Expected<void> MsgBusAmqp::unreceive(const std::string& address)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    if (auto it{m_subScriptions.find(address)}; it != m_subScriptions.end())
    {
      m_subScriptions.at(address)->unreceive();
      m_subScriptions.erase(address);
      logTrace("Unsubscribed for: '{}'", address);
    }
    else
    {
      logError("Unsubscribed '{}' (Rejected)", address);
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }
    return {};
  }

  fty::Expected<void> MsgBusAmqp::send(const Message& message)
  {
    if (!isServiceAvailable())
    {
      logDebug("Service not available");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    }

    logDebug("Sending message {}", message.toString());
    proton::message msgToSend = getAmqpMessage(message);

    auto sender = AmqpClient(m_endpoint);
    std::thread thrd([&]() {
      proton::container(sender).run();
    });
    auto msgSent = sender.send(msgToSend);
    thrd.join();

    if (msgSent != DeliveryState::DELIVERY_STATE_ACCEPTED)
    {
      logError("Message sent (Rejected)");
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }

    logDebug("Message sent (Accepted)");
    return {};
  }

  fty::Expected<Message> MsgBusAmqp::request(const Message& message, int receiveTimeOut)
  {
    try
    {
      if (!isServiceAvailable())
      {
        logDebug("Service not available");
        return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
      }

      logDebug("Sending message {}", message.toString());
      proton::message msgToSend = getAmqpMessage(message);

      AmqpClient requester(m_endpoint);
      std::thread thrd([&]() {
        proton::container(requester).run();
      });
      requester.receive(msgToSend.reply_to(), proton::to_string(msgToSend.correlation_id()));
      thrd.detach();
      send(message);

      MessagePointer response = std::make_shared<proton::message>();
      bool messageArrived = requester.tryConsumeMessageFor(response, receiveTimeOut);

      if (!messageArrived)
      {
        logError("No message arrive in time!");
        return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_TIMEOUT));
      }

      logDebug("Message arrived ({})", proton::to_string(*response));
      return Message{getMetaData(*response), response->body().empty() ? std::string{} : proton::to_string(response->body())};
    }
    catch (std::exception& e)
    {
      return fty::unexpected(e.what());
    }
  }

} // namespace fty::messagebus::amqp
