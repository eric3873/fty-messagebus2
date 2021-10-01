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

#include "fty/messagebus/mqtt/MessageBusMqtt.h"
#include <fty/messagebus/MessageBusStatus.h>

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
                                 const Endpoint& endpoint,
                                 const Message& will)
    : MessageBus()
  {
    m_busMqtt = std::make_shared<MsgBusMqtt>(clientName, endpoint, will);
  }

  MessageBusMqtt::~MessageBusMqtt()
  {
  }

  fty::Expected<void> MessageBusMqtt::connect() noexcept
  {
    return m_busMqtt->connect();
  }

  fty::Expected<void> MessageBusMqtt::send(const Message& msg) noexcept
  {
    if (!msg.isValidMessage())
    {
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    }
    return m_busMqtt->send(msg);
  }

  fty::Expected<void> MessageBusMqtt::receive(const std::string& queue, std::function<void(const Message&)>&& func, const std::string& /*filter*/) noexcept
  {
    return m_busMqtt->receive(queue, func);
  }

  fty::Expected<void> MessageBusMqtt::unreceive(const std::string& address) noexcept
  {
    return m_busMqtt->unreceive(address);
  }

  fty::Expected<Message> MessageBusMqtt::request(const Message& msg, int timeOut) noexcept
  {
    //Sanity check
    if (!msg.isValidMessage())
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    if (!msg.needReply())
      return fty::unexpected(to_string(DeliveryState::DELIVERY_STATE_REJECTED));

    // Sendrequest
    return m_busMqtt->request(msg, timeOut);
  }

  const std::string& MessageBusMqtt::clientName() const noexcept
  {
    return m_busMqtt->clientName();
  }

  static const std::string g_identity(BUS_IDENTITY);

  const std::string& MessageBusMqtt::identity() const noexcept
  {
    return g_identity;
  }

} // namespace fty::messagebus::mqtt
