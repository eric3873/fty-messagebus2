/*  =========================================================================
    MessageBusMqtt.cpp - class description

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

#include "fty/messagebus/mqtt/MessageBusMqtt.h"

#include "CallBack.h"
#include "MsgBusMqtt.h"

#include <mqtt/async_client.h>
#include <mqtt/client.h>
#include <mqtt/message.h>
#include <mqtt/properties.h>

#include <fty/expected.h>
#include <fty_log.h>

#include <memory>

namespace fty::messagebus::mqtt
{
  MessageBusMqtt::MessageBusMqtt(const ClientName& clientName,
                                 const Endpoint& endpoint)
    : MessageBus()
  {
    m_busMqtt = std::make_shared<MsgBusMqtt>(clientName, endpoint);
  }

  MessageBusMqtt::~MessageBusMqtt()
  {}

  fty::Expected<void> MessageBusMqtt::connect() noexcept
  {
    return m_busMqtt->connect();
  }

  fty::Expected<void> MessageBusMqtt::send(const Message& msg) noexcept
  {
    logDebug("Send message");
    //Sanity check
    if(! msg.isValidMessage()) return fty::unexpected(DELIVERY_STATE_REJECTED);

    //Send
    return m_busMqtt->sendRequest(msg.metaData().at(TO), msg);
  }

  fty::Expected<void> MessageBusMqtt::subscribe(const std::string& queue, std::function<void(const Message&)>&& func) noexcept
  {
    return m_busMqtt->subscribe(queue, func);
  }

  fty::Expected<void> MessageBusMqtt::unsubscribe(const std::string& queue) noexcept
  {
    return m_busMqtt->unsubscribe(queue);
  }

  fty::Expected<Message> MessageBusMqtt::request(const Message& msg, int timeOut) noexcept
  {
    //Sanity check
    if(! msg.isValidMessage()) return fty::unexpected(DELIVERY_STATE_REJECTED);
    if(! msg.needReply()) return fty::unexpected(DELIVERY_STATE_REJECTED);

    //Send
    return m_busMqtt->request(msg.metaData().at(TO), msg, timeOut);
  }

  const std::string & MessageBusMqtt::clientName() const noexcept
  {
    return m_busMqtt->clientName();
  }

  static const std::string g_identity(BUS_INDENTITY_MQTT);

  const std::string & MessageBusMqtt::identity() const noexcept
  {
    return g_identity;
  }

} // namespace fty::messagebus::mqtt
