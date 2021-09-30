/*  =========================================================================
    MsgBusMqtt.h - class description

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

#include "CallBack.h"

#include <fty/expected.h>

namespace fty::messagebus::mqtt
{
  class MsgBusMqtt
  {
  public:
    MsgBusMqtt() = delete;

    MsgBusMqtt(const std::string& clientName, const std::string& endpoint, const Message& will = Message())
      : m_clientName(clientName)
      , m_endpoint(endpoint)
      , m_will(will) {};

    ~MsgBusMqtt();

    [[nodiscard]] fty::Expected<void> connect();

    fty::Expected<void> receive(const std::string& address, MessageListener messageListener);
    fty::Expected<void> unreceive(const std::string& address);
    fty::Expected<void> send(const Message& message);

    // Sync request with timeout
    fty::Expected<Message> request(const Message& message, int receiveTimeOut);

    const std::string& clientName() const
    {
      return m_clientName;
    }

  private:
    std::string m_clientName;
    std::string m_endpoint;
    Message m_will;

    // Asynchronous and synchronous mqtt client
    AsynClientPointer m_asynClient;
    SynClientPointer m_synClient;

    // Call back
    CallBack m_cb;
  };
} // namespace fty::messagebus::mqtt
