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
#include <fty/messagebus/Message.h>

#include <fty/messagebus/utils.h>

namespace fty::messagebus
{
  Message::Message(const MetaData& metaData, const UserData& userData)
    : m_metadata(metaData)
    , m_data(userData)
  {
  }

  MetaData& Message::metaData()
  {
    return m_metadata;
  }

  UserData& Message::userData()
  {
    return m_data;
  }

  const MetaData& Message::metaData() const
  {
    return m_metadata;
  }

  const UserData& Message::userData() const
  {
    return m_data;
  }

  bool Message::isValidMessage() const
  {
    //Check that request have all the proper field set
    try
    {
      return ((!m_metadata.at(SUBJECT).empty()) && (!m_metadata.at(FROM).empty()) && (!m_metadata.at(TO).empty()));
    }
    catch (...)
    {
      return false;
    }
  }

  bool Message::isRequest() const
  {
    try
    {
      return ((!m_metadata.at(CORRELATION_ID).empty()) && isValidMessage());
    }
    catch (...)
    {
      return false;
    }
  }

  bool Message::needReply() const
  {
    //Check that request have all the proper field set
    try
    {
      return (!m_metadata.at(REPLY_TO).empty() && isRequest());
    }
    catch (...)
    {
      return false;
    }
  }

  fty::Expected<Message> Message::buildReply(const UserData& userData, const std::string& status) const
  {
    if (!isValidMessage())
      return fty::unexpected("Not a valid message!");
    if (!needReply())
      return fty::unexpected("Nowhere to reply!");

    Message reply;
    reply.m_metadata[FROM] = m_metadata.at(TO);
    reply.m_metadata[TO] = m_metadata.at(REPLY_TO);
    reply.m_metadata[SUBJECT] = m_metadata.at(SUBJECT);
    reply.m_metadata[CORRELATION_ID] = m_metadata.at(CORRELATION_ID);
    reply.m_metadata[STATUS] = status;

    reply.m_data = userData;

    return reply;
  }

  Message Message::buildMessage(const std::string& from, const std::string& to, const std::string& subject, const UserData& userData)
  {
    Message msg;
    msg.m_metadata[FROM] = from;
    msg.m_metadata[TO] = to;
    msg.m_metadata[SUBJECT] = subject;
    msg.m_metadata[CORRELATION_ID] = utils::generateUuid();

    msg.m_data = userData;

    return msg;
  }

  Message Message::buildRequest(const std::string& from, const std::string& to, const std::string& subject, const std::string& replyTo, const UserData& userData)
  {
    Message msg = buildMessage(from, to, subject, userData);
    msg.m_metadata[REPLY_TO] = replyTo;

    return msg;
  }

  std::string Message::toString() const
  {
    std::string data;
    data += "\n=== METADATA ===\n";
    for (auto& [key, value] : m_metadata)
    {
      data += "[" + key + "]=" + value + "\n";
    }
    data += "=== USERDATA ===\n";
    data += m_data;
    data += "\n================";

    return data;
  }

} //namespace fty::messagebus
