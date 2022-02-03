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

#pragma once

#include <fty/expected.h>
#include <map>
#include <string>

namespace fty::messagebus {

using UserData = std::string;
using MetaData = std::map<std::string, std::string>;
using Address  = std::string;

static constexpr auto STATUS_OK = "OK";
static constexpr auto STATUS_KO = "KO";

// Metadata user property
static constexpr auto CORRELATION_ID = "CORRELATION_ID"; // Correlation Id used for request reply pattern
static constexpr auto MESSAGE_ID     = "MESSAGE_ID";     // Message Id
static constexpr auto FROM           = "FROM";           // ClientId of the message
static constexpr auto TO             = "TO";             // Destination queue
static constexpr auto REPLY_TO       = "REPLY_TO";       // Reply queue
static constexpr auto SUBJECT        = "SUBJECT";        // Message subject
static constexpr auto STATUS         = "STATUS";         // Message status

class Message;
class Message
{
public:
    Message() = default;
    Message(const MetaData& metaData, const UserData& userData);
    Message(const Message& message);
    Message(const UserData& userData);

    ~Message() noexcept = default;
    Message& operator   =(const Message& other);

    MetaData&       metaData();
    const MetaData& metaData() const;
    void            metaData(const MetaData& metaData);

    UserData&       userData();
    const UserData& userData() const;
    void            userData(const UserData& userData);

    std::string correlationId() const;
    void        correlationId(const std::string& correlationId);
    std::string from() const;
    void        from(const std::string& from);
    std::string to() const;
    void        to(const std::string& to);
    std::string replyTo() const;
    void        replyTo(const std::string& replyTo);
    std::string subject() const;
    void        subject(const std::string& subject);
    std::string status() const;
    void        status(const std::string& status);
    std::string id() const;
    void        id(const std::string& id);

    std::string getMetaDataValue(const std::string& key) const;
    void        setMetaDataValue(const std::string& key, const std::string& data);

    bool isValidMessage() const;
    bool isRequest() const;
    bool needReply() const;

    fty::Expected<Message> buildReply(const UserData& userData, const std::string& status = STATUS_OK) const;
    static Message         buildMessage(
                const Address& from, const Address& to, const std::string& subject, const UserData& userData = {}, const MetaData& meta = {});
    static Message buildRequest(
        const Address&     from,
        const Address&     to,
        const std::string& subject,
        const Address&     replyTo,
        const UserData&    userData = {},
        const MetaData&    meta     = {});

    MetaData getUndefinedProperties() const;

    std::string toString() const;

protected:
    MetaData m_metadata;
    UserData m_data;
};

} // namespace fty::messagebus
