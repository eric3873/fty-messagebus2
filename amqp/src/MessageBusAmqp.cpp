/*  =========================================================================
    MessageBusAmqp.cpp - class description

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

#include "fty/messagebus/amqp/MessageBusAmqp.h"

#include "MsgBusAmqp.h"

#include <fty/expected.h>
#include <memory>

#include <fty_log.h>

namespace fty::messagebus::amqp
{
  // Topic
  static const std::string PREFIX_TOPIC = "etn.t";

  // Queues
  static const std::string PREFIX_QUEUE = "etn.q.";
  static const std::string PREFIX_REQUEST_QUEUE = PREFIX_QUEUE + "request.";
  static const std::string PREFIX_REPLY_QUEUE = PREFIX_QUEUE + "reply.";

  MessageBusAmqp::MessageBusAmqp(const ClientName& clientName,
                                 const Endpoint& endpoint)
    : MessageBus()
  {
    m_busAmqp = std::make_shared<MsgBusAmqp>(clientName, endpoint);
  }

  MessageBusAmqp::~MessageBusAmqp()
  {
  }

  fty::Expected<void> MessageBusAmqp::connect() noexcept
  {
    return m_busAmqp->connect();
  }

  fty::Expected<void> MessageBusAmqp::send(const Message& msg) noexcept
  {
    logDebug("Send message");
    //Sanity check
    if (!msg.isValidMessage())
    {
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    }

    //Send
    return m_busAmqp->sendRequest(msg.to(), msg);
  }

  fty::Expected<void> MessageBusAmqp::subscribe(const std::string& address, std::function<void(const Message&)>&& func) noexcept
  {
    return m_busAmqp->subscribe(address, func);
  }

  fty::Expected<void> MessageBusAmqp::unsubscribe(const std::string& address) noexcept
  {
    return m_busAmqp->unsubscribe(address);
  }

  fty::Expected<Message> MessageBusAmqp::request(const Message& msg, int timeOut) noexcept
  {
    //Sanity check
    if (!msg.isValidMessage())
      return fty::unexpected(DELIVERY_STATE_REJECTED);
    if (!msg.needReply())
      return fty::unexpected(DELIVERY_STATE_REJECTED);

    //Send
    return m_busAmqp->request(msg.to(), msg, timeOut);
  }

  const std::string& MessageBusAmqp::clientName() const noexcept
  {
    return m_busAmqp->clientName();
  }

  static const std::string g_identity(BUS_INDENTITY_AMQP);

  const std::string& MessageBusAmqp::identity() const noexcept
  {
    return g_identity;
  }

} // namespace fty::messagebus::amqp
