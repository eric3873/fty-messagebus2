/*  =========================================================================
    Copyright (C) 2014 - 2020 Eaton

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

#include <map>
#include <string>

#include <fty/expected.h>

namespace fty::messagebus
{
  using UserData = std::string;
  using MetaData = std::map<std::string, std::string>;

  static constexpr auto STATUS_OK = "OK";
  static constexpr auto STATUS_KO = "KO";

  // Metadata user property
  static constexpr auto CORRELATION_ID = "CORRELATION_ID";
  static constexpr auto FROM = "FROM"; //ClientId of the message
  static constexpr auto TO = "TO"; //Queue of destination
  static constexpr auto REPLY_TO = "REPLY_TO"; //Queue for the reply
  static constexpr auto SUBJECT = "SUBJECT";
  static constexpr auto STATUS = "STATUS";
  static constexpr auto TIMEOUT = "TIMEOUT";

  class Message;
  class Message
  {
  public:
    Message() = default;
    Message(const Message& message);
    Message(const MetaData& metaData, const UserData& userData);

    virtual ~Message() noexcept = default;

    MetaData& metaData();
    const MetaData& metaData() const;
    void metaData(const MetaData& metaData);

    UserData& userData();
    const UserData& userData() const;
    void userData(const UserData& userData);

    std::string correlationId() const;
    void correlationId(const std::string& correlationId);
    std::string from() const;
    void from(const std::string& from);
    std::string to() const;
    void to(const std::string& to);
    std::string replyTo() const;
    void replyTo(const std::string& replyTo);
    std::string subject() const;
    void subject(const std::string& subject);
    std::string status() const;
    void status(const std::string& status);

    std::string getMetaDataValue(const std::string& key) const;
    void setMetaDataValue(const std::string& key, const std::string& data);

    bool isValidMessage() const;
    bool isRequest() const;
    bool needReply() const;

    fty::Expected<Message> buildReply(const UserData& userData, const std::string& status = STATUS_OK) const;
    static Message buildMessage(const std::string& from, const std::string& to, const std::string& subject, const UserData& userData = {}, const MetaData& meta = {});
    static Message buildRequest(const std::string& from, const std::string& to, const std::string& subject, const std::string& replyTo, const UserData& userData = {}, const MetaData& meta = {});

    MetaData getUndefinedProperties() const;

    std::string toString() const;

  protected:
    MetaData m_metadata;
    UserData m_data;
  };

} // namespace fty::messagebus
