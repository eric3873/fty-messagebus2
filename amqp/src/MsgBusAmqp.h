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

#include "AmqpClient.h"

#include <fty/expected.h>

#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>

namespace fty::messagebus::amqp
{

  using MessagePointer = std::shared_ptr<proton::message>;
  using AmqpClientPointer = std::shared_ptr<AmqpClient>;

  class MsgBusAmqp
  {
  public:
    MsgBusAmqp() = delete;

    MsgBusAmqp(const std::string& clientName, const std::string& endpoint)
      : m_clientName(clientName)
      , m_endpoint(endpoint){};

    ~MsgBusAmqp();

    [[nodiscard]] fty::Expected<void> connect();

    fty::Expected<void> receive(const Address& address, MessageListener messageListener, const std::string& filter = {});
    fty::Expected<void> unreceive(const Address& address);
    fty::Expected<void> send(const Message& message);

    // Sync request with timeout
    fty::Expected<Message> request(const Message& message, int receiveTimeOut);

    const std::string& clientName() const
    {
      return m_clientName;
    }

    bool isServiceAvailable();

  private:
    std::string m_clientName{};
    std::string m_endpoint{};

    std::map<std::string, AmqpClientPointer> m_subScriptions;
    AmqpClientPointer m_amqpClient;
  };

} // namespace fty::messagebus::amqp
