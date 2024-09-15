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
#include <fty/messagebus2/Message.h>
#include <fty/messagebus2/utils.h>

namespace fty::messagebus2 {

Message::Message(const MetaData& metaData, const UserData& userData)
    : m_metadata(metaData)
    , m_data(userData)
{
}

Message::Message(const Message& message)
    : Message(message.metaData(), message.userData())
{
}

Message::Message(const UserData& userData)
    : Message({}, userData)
{
}

MetaData& Message::metaData()
{
    return m_metadata;
}

const MetaData& Message::metaData() const
{
    return m_metadata;
}

void Message::metaData(const MetaData& metaData)
{
    m_metadata = metaData;
}

UserData& Message::userData()
{
    return m_data;
}

const UserData& Message::userData() const
{
    return m_data;
}

void Message::userData(const UserData& userData)
{
    m_data = userData;
}

std::string Message::getMetaDataValue(const std::string& key) const
{
    std::string value{};
    if (auto iter{m_metadata.find(key)}; iter != m_metadata.end()) {
        value = iter->second;
    }
    return value;
}

void Message::setMetaDataValue(const std::string& key, const std::string& data)
{
    m_metadata[key] = data;
}

std::string Message::correlationId() const
{
    return getMetaDataValue(CORRELATION_ID);
}

void Message::correlationId(const std::string& correlationId)
{
    setMetaDataValue(CORRELATION_ID, correlationId);
}

std::string Message::from() const
{
    return getMetaDataValue(FROM);
}

void Message::from(const std::string& from)
{
    setMetaDataValue(FROM, from);
}

std::string Message::to() const
{
    return getMetaDataValue(TO);
}

void Message::to(const std::string& to)
{
    setMetaDataValue(TO, to);
}

std::string Message::replyTo() const
{
    return getMetaDataValue(REPLY_TO);
}

void Message::replyTo(const std::string& replyTo)
{
    setMetaDataValue(REPLY_TO, replyTo);
}

std::string Message::subject() const
{
    return getMetaDataValue(SUBJECT);
}

void Message::subject(const std::string& subject)
{
    setMetaDataValue(SUBJECT, subject);
}

std::string Message::status() const
{
    return getMetaDataValue(STATUS);
}

void Message::timeout(const int timeout)
{
    setMetaDataValue(TIME_OUT, std::to_string(timeout));
}

int Message::timeout() const
{
    return std::stoi(getMetaDataValue(TIME_OUT));
}

void Message::status(const std::string& status)
{
    setMetaDataValue(STATUS, status);
}

std::string Message::id() const
{
    return getMetaDataValue(MESSAGE_ID);
}

void Message::id(const std::string& msgId)
{
    setMetaDataValue(MESSAGE_ID, msgId);
}

bool Message::isValidMessage() const
{
    return ((!subject().empty()) && (!from().empty()) && (!to().empty()));
}

bool Message::isRequest() const
{
    return ((!correlationId().empty()) && isValidMessage());
}

bool Message::needReply() const
{
    // Check that request have all the proper field set
    return (!replyTo().empty() && isRequest());
}

Message Message::buildMessage(
    const Address& from, const Address& to, const std::string& subject, const UserData& userData, const MetaData& meta)
{
    Message msg;
    msg.metaData(meta);

    msg.from(from);
    msg.to(to);
    msg.subject(subject);
    msg.userData(userData);

    return msg;
}

Message Message::buildRequest(
    const Address&     from,
    const Address&     to,
    const std::string& subject,
    const Address&     replyTo,
    const UserData&    userData,
    const MetaData&    meta,
    const int          timeout_s)
{
    Message msg = buildMessage(from, to, subject, userData, meta);
    msg.replyTo(replyTo);
    msg.correlationId(utils::generateUuid());
    // Set timeout in ms
    msg.timeout(timeout_s * 1000);

    return msg;
}

fty::Expected<Message> Message::buildReply(const UserData& userData, const std::string& status) const
{
    if (!isValidMessage())
        return fty::unexpected("Not a valid message!");
    if (!needReply())
        return fty::unexpected("No where to reply!");

    Message reply;
    reply.from(to());
    reply.to(replyTo());
    reply.subject(subject());
    reply.correlationId(correlationId());
    reply.status(status);
    reply.userData(userData);

    return reply;
}

MetaData Message::getUndefinedProperties() const
{
    MetaData metaData;
    for (const auto& [key, value] : m_metadata) {
        if (key != CORRELATION_ID && key != FROM && key != TO && key != REPLY_TO && key != SUBJECT) {
            metaData.emplace(key, value);
        }
    }
    return metaData;
}

std::string Message::toString() const
{
    std::string data;
    data += "\nfrom=" + from();
    data += "\nto=" + to();
    data += "\nsubject=" + subject();
    data += "\ncorrelationId=" + correlationId();
    data += "\nstatus=" + status();
    data += "\n=== METADATA ===\n";
    for (auto& [key, value] : m_metadata) {
        data += "[" + key + "]=" + value + "\n";
    }
    data += "=== USERDATA ===\n";
    data += m_data;
    data += "\n================";

    return data;
}

Message& Message::operator=(const Message& other)
{
    m_metadata = std::move(other.metaData());
    m_data     = std::move(other.userData());
    return *this;
}

} // namespace fty::messagebus2
