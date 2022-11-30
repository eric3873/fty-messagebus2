/*  =========================================================================
    Copyright (C) 2014 - 2022 Eaton

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

#include "fty/messagebus2/Message.h"

#include <qpid/messaging/Address.h>
#include <qpid/messaging/Message.h>

#include <iostream>

namespace fty::messagebus2::amqp {

inline const std::string sanitizeAddress(const Address& addressIn)
{
    // Add simple quotes around the address, e.g. "queue://myQueue" -> "'queue://myQueue'"
    std::string address { addressIn };
    if (!address.empty() && address[0] != '\'') {
        address = "'" + addressIn + "'";
    }
    return address;
}

inline const std::string qpidMsgtoString(const qpid::messaging::Message& qpidMsg)
{
    std::string data;
    data += "\nfrom=" + qpidMsg.getMessageId();
    data += "\nsubject=" + qpidMsg.getSubject();
    data += "\ncorrelationId=" + qpidMsg.getCorrelationId();
    data += "\ngetReplyTo=" + qpidMsg.getReplyTo().str();
    data += "\n=== METADATA ===\n";
    for (const auto& [key, variant] : qpidMsg.getProperties()) {
      data += "[" + key + "]=" + variant.asString() + "\n";
    }
    data += "=== USERDATA ===\n";
    data += qpidMsg.getContent();
    data += "\n================";

    return data;
}

inline const std::string getAddress(const qpid::messaging::Message& qpidMsg)
{
    // Note: destination is not directly accessible in message.
    // It must be extracted from "x-amqp-to" key in properties.
    auto search = qpidMsg.getProperties().find("x-amqp-to");
    if (search != qpidMsg.getProperties().end()) {
        auto address = search->second.getString();
        return address;
    }
    return "";
}

inline const Message getMessage(const qpid::messaging::Message& qpidMsg)
{
    Message message;

    // User properties
    if (!qpidMsg.getProperties().empty()) {
        for (const auto& [key, value] : qpidMsg.getProperties()) {
            message.metaData().emplace(key, value);
        }
    }

    if (!qpidMsg.getUserId().empty()) {
        message.from(qpidMsg.getUserId());
    }
    if (!qpidMsg.getMessageId().empty()) {
        message.id(qpidMsg.getMessageId());
        message.from(qpidMsg.getMessageId());
    }
    if (!qpidMsg.getSubject().empty()) {
        message.subject(qpidMsg.getSubject());
    }
    // Req/Rep pattern properties
    if (!qpidMsg.getCorrelationId().empty()) {
        message.correlationId(qpidMsg.getCorrelationId());
    }
    if (!qpidMsg.getReplyTo().str().empty()) {
        message.replyTo(qpidMsg.getReplyTo().str());
    }
    message.to(getAddress(qpidMsg));
    message.userData(qpidMsg.getContent());
    return message;
}

inline const qpid::messaging::Message getAmqpMessage(const Message& message)
{
    qpid::messaging::Message qpidMsg;

    // Fill in replyTo and CorrelationId only if there are not empty, otherwise filled in with empty values,
    // the filtering on correlationId with library does not work.
    if (!message.replyTo().empty()) {
        qpidMsg.setReplyTo(message.replyTo());
    }
    if (!message.correlationId().empty()) {
        qpidMsg.setCorrelationId(message.correlationId());
    }
    // Destination is not directly settable in message.
    // It must be saved in the "x-amqp-to" key of properties.
    qpidMsg.getProperties()["x-amqp-to"] = message.to();
    qpidMsg.setSubject(message.subject());
    qpidMsg.setUserId(message.from());
    qpidMsg.setMessageId(message.from());

    // All remaining properties
    for (const auto& [key, value] : message.getUndefinedProperties()) {
        qpidMsg.getProperties()[key] = value;
    }

    // TBD Set content type (i.e. MIME type)
    qpidMsg.setContentType("text/plain");
    qpidMsg.setContent(message.userData());
    return qpidMsg;
}

} // namespace fty::messagebus2::amqp
