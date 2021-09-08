
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

/*
@header
    fty_common_messagebus_mqtt -
@discuss
@end
*/

#include "fty/messagebus/amqp/MsgBusAmqp.hpp"

//#include "fty/messagebus/amqp/MsgBusAmqpMessage.hpp"
#include "fty/messagebus/amqp/MsgBusAmqpUtils.hpp"

#include <fty/messagebus/MsgBusException.hpp>

#include <fty/messagebus/utils/MsgBusHelper.hpp>

// #include <fty/messagebus/amqp/AmqpSender.hpp>
// #include <fty/messagebus/amqp/AmqpContainer.hpp>
// #include <fty/messagebus/amqp/Client.hpp>
// #include <fty/messagebus/amqp/Replyer.hpp>
#include <fty/messagebus/amqp/Requester.hpp>

#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>

#include <fty_log.h>
#include <iostream>

namespace fty::messagebus::amqp
{
  static auto constexpr AMQP_QUEUE_PREFIX = "queue://";
  static auto constexpr AMQP_TOPIC_PREFIX = "topic://";

  using Message = fty::messagebus::amqp::AmqpMessage;
  using proton::receiver_options;
  using proton::source_options;

  MessageBusAmqp::~MessageBusAmqp()
  {
    // Cleaning all amqp clients
    if (true /*isServiceAvailable()*/)
    {
      log_debug("Cleaning for: %s", m_clientName.c_str());
      try
      {
        m_container->stop();
        log_debug("Container stoped");
        // std::for_each(m_containerThreads.begin(), m_containerThreads.end(), [](std::thread& t) {
        //   t.join();
        // });
        // for (const auto& [key, pHandle] : m_containerThreads) {
        //     std::cout << "key: " << key << std::endl;
        //     int result = pthread_cancel(pHandle);
        //     std::cout << "cancel: " << strerror(result) << std::endl;
        // }
        pthread_cancel(m_containerThreads["sub"]);
        //pthread_cancel(m_containerThreads["pub"]);
        pthread_cancel(m_containerThreads["container"]);
        log_debug("%s cleaned", m_clientName.c_str());
      }
      catch (const std::exception& e)
      {
        log_error("Exception: %s", e.what());
      }
    }
    else
    {
      log_error("Cleaning error for: %s", m_clientName.c_str());
    }
  }

  ComState MessageBusAmqp::connect()
  {
    auto status = ComState::COM_STATE_NO_CONTACT;
    try
    {
      auto clientId = utils::getClientId(m_clientName);
      log_debug("Amqp connecting for clientId: %s", clientId.c_str());

      m_client = std::make_shared<Client>(DEFAULT_AMQP_END_POINT);
      m_client->senderAddress("topic://examples");

      m_container = std::make_shared<proton::container>(*m_client);
      // m_containerThreads.push_back(std::thread([=]() {
      //   m_container->run();
      // }).native_handle());

      std::thread thrd([=]() {
        m_container->run();
      });
      // //m_containerThreads.push_back(thrd.native_handle());
      m_containerThreads["container"] = thrd.native_handle();
      thrd.detach();

      //m_containerThreads.front().detach();

      //if (m_client->connectionActive())
      // {
      // TODO synchronize with client
      std::this_thread::sleep_for(std::chrono::seconds(1));
      status = ComState::COM_STATE_OK;
      // }
    }
    catch (const std::exception& e)
    {
      status = ComState::COM_STATE_UNKNOWN;
      log_error("Error to connect with the Amqp server, reason: %s", e.what());
    }

    log_debug("Status %d", status);
    return status;
  }

  bool MessageBusAmqp::isServiceAvailable()
  {
    bool serviceAvailable = true;
    // if (!m_client.connection().active())
    // {
    //   log_error("Amqp service is unvailable");
    //   serviceAvailable = false;
    // }
    return serviceAvailable;
  }

  DeliveryState MessageBusAmqp::publish(const std::string& topic, const Message& message)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;
    if (isServiceAvailable())
    {
      proton::message pMsg(message.userData());
      std::string amqpTopic = AMQP_TOPIC_PREFIX + topic;
      log_debug("Publishing [%s] on: %s", message.userData().c_str(), amqpTopic.c_str());

      m_client->senderAddress(amqpTopic);
      try
      {
        std::thread senderTh([&]() {
          m_client->send(pMsg);
        });
        // m_containerThreads["pub"] = senderTh.native_handle();
        // senderTh.detach();
        senderTh.join();
      }
      catch (const std::exception& e)
      {
        log_error("Error on publishing: %s", e.what());
      }

      delivState = DeliveryState::DELI_STATE_ACCEPTED;
      log_debug("Message published (%s)", to_string(delivState).c_str());
    }
    return delivState;
  }

  DeliveryState MessageBusAmqp::subscribe(const std::string& topic, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;
    if (isServiceAvailable())
    {
      std::string amqpTopic = AMQP_TOPIC_PREFIX + topic;
      log_debug("Subscribing on topic: %s", amqpTopic.c_str());
      m_client->receiverAddress(amqpTopic);

      std::thread receiveTh([&]() {
        auto msg = m_client->receive();
        OUT(std::cout << "received \"" << msg.body() << '"' << std::endl);
      });

      m_containerThreads["sub"] = receiveTh.native_handle();
      receiveTh.detach();

      delivState = DeliveryState::DELI_STATE_ACCEPTED;
      log_debug("Subscribed (%s)", to_string(delivState).c_str());
    }
    return delivState;
  }

  DeliveryState MessageBusAmqp::unsubscribe(const std::string& /*topic*/, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  DeliveryState MessageBusAmqp::receive(const std::string& queue, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_ACCEPTED; //DeliveryState::DELI_STATE_UNAVAILABLE;
    std::string amqpQueue = AMQP_QUEUE_PREFIX + queue;

    // Replyer replyer(m_endPoint, amqpQueue);
    // proton::container(replyer).run();
    log_debug("Waiting to receive msg from: %s", amqpQueue.c_str(), to_string(delivState).c_str());

    return delivState;
  }

  DeliveryState MessageBusAmqp::sendRequest(const std::string& requestQueue, const Message& message)
  {
    auto delivState = DeliveryState::DELI_STATE_ACCEPTED; //DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  DeliveryState MessageBusAmqp::sendRequest(const std::string& requestQueue, const Message& message, MessageListener messageListener)
  {
    auto delivState = receive(requestQueue, messageListener);
    if (delivState == DELI_STATE_ACCEPTED)
    {
      delivState = sendRequest(requestQueue, message);
    }
    return delivState;
  }

  DeliveryState MessageBusAmqp::sendReply(const std::string& /*replyQueue*/, const Message& /*message*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  Opt<Message> MessageBusAmqp::request(const std::string& requestQueue, const Message& message, int receiveTimeOut)
  {
    auto replyMsg = Opt<Message>{};

    auto delivState = DeliveryState::DELI_STATE_ACCEPTED; //DeliveryState::DELI_STATE_UNAVAILABLE;
    std::string amqpQueue = AMQP_QUEUE_PREFIX + requestQueue;

    std::string replyTo = AMQP_QUEUE_PREFIX + fty::messagebus::amqp::getReplyQueue(message);
    auto protonMsg = getAmqpMessageFromMsgBusAmqpMessage(message);
    // TODO remove from here
    protonMsg.to(amqpQueue);

    std::cout << protonMsg << std::endl;

    Requester requester(m_endPoint, protonMsg, receiveTimeOut);
    proton::container(requester).run();
    log_debug("Sending request payload: '%s' to: %s and wait message on reply queue %s", message.userData().c_str(), amqpQueue.c_str(), replyTo.c_str());

    return replyMsg;
  }

} // namespace fty::messagebus::amqp
