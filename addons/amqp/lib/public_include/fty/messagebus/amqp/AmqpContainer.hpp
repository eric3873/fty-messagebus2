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

#include <iostream>

namespace fty::messagebus::amqp
{

  class AmqpContainer : public proton::messaging_handler
  {
    // Invariant
    const std::string m_url;

  public:
    AmqpContainer(const std::string& url)
      : m_url(url){};

    ~AmqpContainer() = default;

    void on_container_start(proton::container& cont) override
    {
      std::cout << "AmqpContainer on_container_start " << std::endl;
      cont.connect(m_url);
    }

    void on_connection_open(proton::connection& conn) override
    {
      std::cout << "on_connection_open " << std::endl;
      std::cout << "on_connection_open is active:" << conn.active() << std::endl;
      //conn.open_sender("examples");
    }

    void on_sender_open(proton::sender& s) override
    {
      // sender_ and work_queue_ must be set atomically
      std::cout << "on_sender_open " << std::endl;
    }

    void on_sendable(proton::sender& s)
    {
      std::cout << "on_sendable" << std::endl;

      std::cout << "fin on_sendable" << std::endl;
    }

    void on_error(const proton::error_condition& e) override
    {
      std::cerr << "unexpected error: " << e << std::endl;
    }

    void on_tracker_accept(proton::tracker& t) override
    {
      std::cout << "on_tracker_accept" << std::endl;

    }

    void on_transport_close(proton::transport&) override
    {
      std::cout << "on_transport_close" << std::endl;
    }
  };

} // namespace fty::messagebus::amqp
