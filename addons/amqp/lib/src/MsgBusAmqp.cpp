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

#include <fty/messagebus/amqp/MsgBusAmqp.hpp>
#include <fty/messagebus/MsgBusException.hpp>
#include <fty/messagebus/utils/MsgBusHelper.hpp>


#include <proton/connection_options.hpp>
#include <proton/container.hpp>


#include <fty_log.h>

namespace fty::messagebus::amqp
{
  using Message = fty::messagebus::amqp::AmqpMessage;

  MessageBusAmqp::~MessageBusAmqp()
  {
    // Cleaning all async clients
    if (isServiceAvailable())
    {
      log_debug("Cleaning for: %s", m_clientName.c_str());
    }
    else
    {
      log_error("Cleaning error for: %s", m_clientName.c_str());
    }
  }

  ComState MessageBusAmqp::connect()
  {
    auto status = ComState::COM_STATE_NO_CONTACT;

    log_debug("Amqp connect");


    m_client = std::make_shared<AmqpClient>(m_endPoint, "examples");
    proton::container container(*m_client);
    // TODO ste more, or appropriate connection options
    container.client_connection_options(proton::connection_options().max_frame_size(12345).idle_timeout(proton::duration(15000)));
    container.run();

    if (m_client->connectionActive())
    {
      status = ComState::COM_STATE_OK;
    }

    return status;
  }

  bool MessageBusAmqp::isServiceAvailable()
  {
    bool serviceAvailable = false;
    if (!serviceAvailable)
    {
      log_error("Amqp service is unvailable");
    }
    return serviceAvailable;
  }

  DeliveryState MessageBusAmqp::publish(const std::string& /*topic*/, const Message& /*message*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

    return delivState;
  }

  DeliveryState MessageBusAmqp::subscribe(const std::string& /*topic*/, MessageListener /*messageListener*/)
  {
    auto delivState = DeliveryState::DELI_STATE_UNAVAILABLE;

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
