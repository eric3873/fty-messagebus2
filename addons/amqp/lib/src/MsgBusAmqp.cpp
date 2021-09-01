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

#include <fty/messagebus/MsgBusException.hpp>
#include <fty/messagebus/amqp/MsgBusAmqp.hpp>
#include <fty/messagebus/utils/MsgBusHelper.hpp>
// #include <fty/messagebus/amqp/AmqpSender.hpp>
#include <fty/messagebus/amqp/Client.hpp>

#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>

#include <fty_log.h>
#include <iostream>

using proton::receiver_options;
using proton::source_options;

namespace fty::messagebus::amqp
{
  using Message = fty::messagebus::amqp::AmqpMessage;

  MessageBusAmqp::~MessageBusAmqp()
  {
    // Cleaning all async clients
    if (true /*isServiceAvailable()*/)
    {
      log_debug("Cleaning for: %s", m_clientName.c_str());
      try
      {
        //m_client->connection().close();
        //m_container->stop();
        container_.stop();
        log_debug("stop done");
        //m_containerThread.join();
        std::for_each(m_containerThreads.begin(), m_containerThreads.end(), [](std::thread& t) {
          t.join();
        });

        log_debug("Cleaned for: %s", m_clientName.c_str());
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

      log_debug("Amqp connect");

      //m_client = std::make_shared<AmqpClient>(m_endPoint, "examples");

      // AmqpClient client(m_endPoint, "examples");
      auto clientId = utils::getClientId(m_clientName);
      log_debug("clientId: %s", clientId.c_str());

      m_container = std::make_shared<proton::container>();
      //m_container = std::make_shared<proton::container>(*m_client, clientId);
      //m_container = std::make_shared<proton::container>(client, clientId);

      //m_container = std::make_shared<proton::container>(*m_client, utils::getClientId(m_clientName));
      //m_container = std::make_shared<proton::container>(utils::getClientId(m_clientName));
      /*proton::container*/ //m_container(client, utils::getClientId(m_clientName));
      //m_container = aContainer;
      // auto amqpClient = AmqpClient(m_endPoint, "examples");
      // m_container(amqpClient);
      // TODO ste more, or appropriate connection options
      //m_container->client_connection_options(proton::connection_options().max_frame_size(12345).idle_timeout(proton::duration(15000)));

      //proton::connection connection = m_container->connect(m_endPoint);
      //m_container->open_receiver("sample");
      //m_container->run();
      //proton::container(client).run(std::thread::hardware_concurrency());

      //proton::container container(client);
      //std::thread containerThread([&]() { m_container->run(); });

      m_containerThreads.push_back(std::thread([=]() {
        //m_container->run();
        container_.run();
      }));

      // client cl(m_endPoint, "examples");
      // proton::container container(cl);
      // std::thread container_thread([&]() { container.run(); });

      //m_containerThreads.emplace_back(&containerThread);
      //m_containerThread([&]() { m_container->run(); });
      // m_containerThread.detach();//join();
      //m_container->open_sender("examples");

      log_debug("After run");
      //if (connection.active())
      // //if (client.connectionActive())
      // {
      status = ComState::COM_STATE_OK;
      // }
    }
    catch (const std::exception& e)
    {
      status = ComState::COM_STATE_OK; // ComState::COM_STATE_UNKNOWN;
      log_error("Error to connect with the Amqp server, reason: %s", e.what());

      // std::cerr << e.what() << '\n';
    }

    log_debug("Status %d", status);
    return status;
  }

  bool MessageBusAmqp::isServiceAvailable()
  {
    bool serviceAvailable = true;
    if (!m_client->connection().active())
    {
      log_error("Amqp service is unvailable");
      serviceAvailable = false;
    }
    return true; //serviceAvailable;
  }

  DeliveryState MessageBusAmqp::publish(const std::string& topic, const Message& message)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;
    if (true /*isServiceAvailable()*/)
    {
      proton::message pMsg(message.userData().c_str());
      log_debug("Publishing [%s] on topic: %s", message.userData().c_str(), topic.c_str());
      //sender send(*m_container, m_endPoint, topic);
      //sender send(container_, m_endPoint, topic);
      AmqpClient client(*m_container, m_endPoint, topic, pMsg);
      proton::container container(client);
      container.run();

      // client cl(m_endPoint, "examples");
      // proton::container container(cl);
      // std::thread container_thread([&]() { container.run(); });

      // std::thread sender([&]() {
      //     cl.send(pMsg);
      // });

      try
      {
        //client.send(pMsg);
      }
      catch (const std::exception& e)
      {
        log_error("Error on publishing: %s", e.what());
      }

      delivState = DeliveryState::DELI_STATE_ACCEPTED;
      log_debug("Message published (%s)", to_string(delivState).c_str());
    }
    return delivState;
  } // namespace fty::messagebus::amqp

  DeliveryState MessageBusAmqp::subscribe(const std::string& topic, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;
    if (isServiceAvailable())
    {
      log_debug("Subscribing on topic: %s", topic.c_str());

      log_debug("Subscribed (%s)", to_string(delivState).c_str());
    }
    return delivState;
  }

  DeliveryState MessageBusAmqp::unsubscribe(const std::string& /*topic*/, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  DeliveryState MessageBusAmqp::receive(const std::string& /*queue*/, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  DeliveryState MessageBusAmqp::sendRequest(const std::string& /*requestQueue*/, const Message& /*message*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

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

  Opt<Message> MessageBusAmqp::request(const std::string& /*requestQueue*/, const Message& /*message*/, int /*receiveTimeOut*/)
  {
    auto replyMsg = Opt<Message>{};

    return replyMsg;
  }

} // namespace fty::messagebus::amqp
