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

#include <fty/messagebus/MsgBusAmqp.hpp>

// TODO copy/paste from MsgBusMqtt.cpp, if in the real implementation, it's the same factorize it with Mqtt

namespace fty::messagebus
{
  using AmqpMessage = fty::messagebus::amqp::AmqpMessage;
  using UserData = fty::messagebus::amqp::UserData;

  static constexpr auto AMQP_IMPL = "Message bus above Amqp implementation";

  // Topic
  static const std::string PREFIX_TOPIC = "etn.t";

  // Queues
  static const std::string PREFIX_QUEUE = "etn.t.";
  static const std::string PREFIX_REQUEST_QUEUE = PREFIX_QUEUE + "request";
  static const std::string PREFIX_REPLY_QUEUE = PREFIX_QUEUE + "reply";
}

namespace fty::messagebus
{

  MsgBusAmqp::MsgBusAmqp(const ClientName& clientName, const Endpoint& endpoint)
    : MsgBusWrapper(clientName, endpoint, AMQP_IMPL)
  {
  }

  DeliveryState MsgBusAmqp::subscribe(const std::string& topic, MessageListener<AmqpMessage> messageListener)
  {
    return m_msgBus->subscribe(/*PREFIX_TOPIC + */topic, messageListener);
  }

  DeliveryState MsgBusAmqp::unsubscribe(const std::string& topic)
  {
    return m_msgBus->unsubscribe(/*PREFIX_TOPIC + */topic, nullptr);
  }

  DeliveryState MsgBusAmqp::publish(const std::string& topic, const UserData& msg)
  {
    AmqpMessage message;
    message.userData() = msg;
    message.metaData().clear();
    message.metaData().emplace(SUBJECT, PUBLISH_USER_PROPERTY);
    message.metaData().emplace(FROM, clientName());

    return m_msgBus->publish(/*PREFIX_TOPIC + */topic, message);
  }

  DeliveryState MsgBusAmqp::sendRequest(const std::string& requestQueue, const UserData& request, MessageListener<AmqpMessage> messageListener)
  {
    auto message = buildMessage(requestQueue, request);
    m_msgBus->receive(message.metaData().find(REPLY_TO)->second, messageListener);
    return m_msgBus->sendRequest(/*PREFIX_REQUEST_QUEUE + */requestQueue, message);
  }

  Opt<AmqpMessage> MsgBusAmqp::sendRequest(const std::string& requestQueue, const UserData& request, int timeOut)
  {
    return m_msgBus->request(/*PREFIX_REQUEST_QUEUE + */requestQueue, buildMessage(requestQueue, request), timeOut);
  }

  DeliveryState MsgBusAmqp::registerRequestListener(const std::string& requestQueue, MessageListener<AmqpMessage> messageListener)
  {
    return m_msgBus->receive(/*PREFIX_REQUEST_QUEUE + */requestQueue, messageListener);
  }

  DeliveryState MsgBusAmqp::sendRequestReply(const AmqpMessage& inputRequest, const UserData& response)
  {
    AmqpMessage responseMsg;
    responseMsg.userData() = response;
    responseMsg.metaData().emplace(STATUS, STATUS_OK);
    responseMsg.metaData().emplace(SUBJECT, ANSWER_USER_PROPERTY);
    responseMsg.metaData().emplace(FROM, clientName());
    responseMsg.metaData().emplace(REPLY_TO, inputRequest.metaData().find(REPLY_TO)->second);
    responseMsg.metaData().emplace(CORRELATION_ID, inputRequest.metaData().find(CORRELATION_ID)->second);

    return m_msgBus->sendReply(inputRequest.metaData().find(REPLY_TO)->second, responseMsg);
  }

  AmqpMessage MsgBusAmqp::buildMessage(const std::string& queue, const UserData& msg)
  {
    auto correlationId = utils::generateUuid();
    auto replyTo = /*PREFIX_REPLY_QUEUE +*/ queue + '.' + correlationId;

    AmqpMessage message;
    message.userData() = msg;
    message.metaData().clear();
    message.metaData().emplace(SUBJECT, QUERY_USER_PROPERTY);
    message.metaData().emplace(FROM, clientName());
    message.metaData().emplace(REPLY_TO, replyTo);
    message.metaData().emplace(CORRELATION_ID, correlationId);

    return message;
  }
} // namespace fty::messagebus
