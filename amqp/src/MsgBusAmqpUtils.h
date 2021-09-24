/*  =========================================================================
    MsgBusAmqpUtils.h - class description

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

#pragma once

#include "fty/messagebus/Message.h"

#include <proton/message.hpp>
#include <proton/message_id.hpp>
#include <proton/scalar_base.hpp>
#include <proton/types.hpp>

namespace fty::messagebus::amqp
{
  using property_map = std::map<std::string, proton::scalar>;

  inline const MetaData getMetaData(const proton::message& protonMsg)
  {
    Message message;

    // User properties
    if (!protonMsg.properties().empty())
    {
      property_map props;
      proton::get(protonMsg.properties(), props);
      for (property_map::iterator it = props.begin(); it != props.end(); ++it)
      {
        message.metaData().emplace(proton::to_string(it->first), proton::to_string(it->second));
      }
    }

    if (!protonMsg.user().empty())
    {
      message.from(protonMsg.user());
    }

    if (!protonMsg.id().empty())
    {
      message.from(proton::to_string(protonMsg.id()));
    }

    if (!protonMsg.subject().empty())
    {
      message.subject(protonMsg.subject());
    }

    // Req/Rep pattern properties
    if (!protonMsg.correlation_id().empty())
    {
      message.correlationId(proton::to_string(protonMsg.correlation_id()));
    }

    if (!protonMsg.address().empty())
    {
      message.replyTo(protonMsg.reply_to());
    }

    if (!protonMsg.to().empty())
    {
      message.to(protonMsg.to());
    }

    return message.metaData();
  }

  inline const proton::message getAmqpMessage(const Message& message)
  {
    proton::message protonMsg;

    if(!message.replyTo().empty())
    {
      protonMsg.correlation_id(message.correlationId());
      protonMsg.reply_to(message.replyTo());
      protonMsg.to(message.replyTo());
    }

    if(!message.subject().empty())
    {
      protonMsg.subject(message.subject());
    }

    if(!message.to().empty())
    {
      protonMsg.subject(message.to());
    }

    if(!message.from().empty())
    {
      protonMsg.user(message.from());
      protonMsg.id(message.from());
    }

    // All remaining properties
    for (const auto& [key, value] : message.getUndefinedProperties())
    {
      protonMsg.properties().put(key, value);
    }

    protonMsg.content_type("string");
    protonMsg.body(message.userData());
    return protonMsg;
  }

} // namespace fty::messagebus::amqp
