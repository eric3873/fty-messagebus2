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

/*
@header
    MsgBusAmqpCallBack.cpp -
@discuss
@end
*/

#include "fty/messagebus/amqp/MsgBusAmqpClient.hpp"
#include "fty/messagebus/amqp/MsgBusAmqpMessage.hpp"

#include <fty_log.h>
#include <iostream>

namespace fty::messagebus::amqp
{

  void AmqpClient::on_container_start(proton::container& c)
  {
    std::cout << "on_container_start" << std::endl;
    proton::connection connection = c.connect(m_url);
    m_connectionActive = connection.active();
    std::cout << "connection active: " << m_connectionActive << std::endl;
  }

  void AmqpClient::on_connection_open(proton::connection& c)
  {
    std::cout << "on_connection_open" << std::endl;
    c.open_receiver(m_addr);
    c.open_sender(m_addr);
  }

  void AmqpClient::on_sendable(proton::sender& s)
  {
    proton::message m("Hello World!");
    s.send(m);
    s.close();
  }

  void AmqpClient::on_message(proton::delivery& d, proton::message& m)
  {
    std::cout << "on_message" << std::endl;
    std::cout << m.body() << std::endl;
    d.connection().close();
  }

  bool AmqpClient::connectionActive()
  {
    return m_connectionActive;
  }

} // namespace fty::messagebus::amqp
