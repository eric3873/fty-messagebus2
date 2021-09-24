/*  =========================================================================
    MsgBusAmqp.h - class description

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

#include "Receiver.h"
#include "Requester.h"
#include "Sender.h"

#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/listen_handler.hpp>

namespace fty::messagebus::amqp
{

  using Container = proton::container;
  //using ClientPointer = std::shared_ptr<Client>;
  using ContainerPointer = std::shared_ptr<Container>;
  using MessagePointer = std::shared_ptr<proton::message>;
  using ReceiverPointer = std::shared_ptr<Receiver>;
  using SenderPointer = std::shared_ptr<Sender>;

  class MsgBusAmqp
  {
  public:
    MsgBusAmqp() = delete;

    MsgBusAmqp(const std::string& clientName, const std::string& endpoint)
      : m_clientName(clientName)
      , m_endpoint(endpoint){};

    ~MsgBusAmqp();

    [[nodiscard]] fty::Expected<void> connect();

    fty::Expected<void> send(const Message& message);
    fty::Expected<void> receive(const std::string& address, MessageListener messageListener, const std::string& filter = {});
    fty::Expected<void> unreceive(const std::string& address);

    // Sync request with timeout
    fty::Expected<Message> request(const Message& message, int receiveTimeOut);

    const std::string& clientName() const
    {
      return m_clientName;
    }

  private:
    std::string m_clientName{};
    std::string m_endpoint{};

    std::map<std::string, ReceiverPointer> m_subScriptions;
  };

} // namespace fty::messagebus::amqp
