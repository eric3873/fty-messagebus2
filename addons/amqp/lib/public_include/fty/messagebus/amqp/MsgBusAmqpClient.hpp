/*  =========================================================================
    fty_common_messagebus_mqtt - class description

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

#include <fty/messagebus/IMessageBus.hpp>

#include <map>
#include <proton/connection.hpp>
#include <proton/container.hpp>
#include <proton/delivery.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver_options.hpp>
#include <proton/source_options.hpp>
#include <proton/tracker.hpp>
#include <string>
#include <thread>

namespace fty::messagebus::amqp
{

  class AmqpClient : public proton::messaging_handler
  {
  public:
    AmqpClient(const std::string& url, const std::string& addr)
      : m_url(url)
      , m_addr(addr){};

    ~AmqpClient() = default;
    void on_container_start(proton::container& c) override;
    void on_connection_open(proton::connection& c) override;
    void on_sendable(proton::sender& s) override;
    void on_message(proton::delivery& d, proton::message& m) override;

    bool connectionActive();


  private:
    std::string m_url;
    std::string m_addr;
    bool m_connectionActive;


  };

} // namespace fty::messagebus::amqp
