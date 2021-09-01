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

#include <proton/connection.hpp>
#include <proton/connection_options.hpp>
#include <proton/container.hpp>
#include <proton/message.hpp>
#include <proton/messaging_handler.hpp>
#include <proton/receiver.hpp>
#include <proton/receiver_options.hpp>
#include <proton/sender.hpp>
#include <proton/work_queue.hpp>

#include <fty_log.h>
#include <iostream>

namespace fty::messagebus::amqp
{
  // AmqpClient::AmqpClient(proton::container& cont, const std::string& url, const std::string& address)
  // {
  //   std::cout << url + "/" + address << std::endl;
  //   proton::connection connection = cont.connect(url);
  //   std::cout << "connection active: " << connection.active() << std::endl;
  //   //m_connection = c.connect(m_url);
  //   //m_connectionActive = m_connection.active();
  //   cont.open_sender(url + "/" + address, proton::connection_options().handler(*this));
  // }

  AmqpClient::~AmqpClient()
  {
    std::cout << "~AmqpClient()" << std::endl;
    if (m_connection.active())
    {
      std::cout << "~AmqpClient() close" << std::endl;
      m_connection.close();
    }
    std::cout << "fin ~AmqpClient()" << std::endl;
  }



  void AmqpClient::on_container_start(proton::container& c)
  {
    std::cout << "on_container_start" << std::endl;
    //proton::connection connection = c.connect(m_url);
    m_connection = c.connect(m_url);
    //m_connectionActive = m_connection.active();
    //std::cout << "connection active: " << m_connectionActive << std::endl;

    // sender = c.open_sender(m_url);
    // proton::receiver_options opts = proton::receiver_options().source(proton::source_options().dynamic(true));
    // receiver = sender.connection().open_receiver("", opts);
  }

  void AmqpClient::on_connection_open(proton::connection& c)
  {
    std::cout << "on_connection_open" << std::endl;
    // c.open_receiver(m_addr);
    c.open_sender(m_addr);
    //return;
  }

  void AmqpClient::on_sender_open(proton::sender& s)
  {
    std::cout << "on_sender_open " << std::endl;
    //sender = s;
    std::cout << "fin on_sender_open " << std::endl;
  }

  void AmqpClient::on_sendable(proton::sender& s)
  {
    std::cout << "on_sendable" << std::endl;
    //proton::message m("Hello World!");
    s.send(m_msg);
    s.close();
    m_connection.close();
    std::cout << "fin on_sendable" << std::endl;
  }

  void AmqpClient::send(const proton::message& m)
  {
    std::cout << "Sending: " << proton::to_string(m) << std::endl;
    sender.send(m);
    m_connection.close();
  }

  void AmqpClient::on_message(proton::delivery& d, proton::message& m)
  {
    std::cout << "on_message" << std::endl;
    //std::cout << m.body() << std::endl;
    //std::cout << "isConnectionActive() " << m_connection.active() << std::endl;
    //d.connection().close();
    //   //d.accept();

    //   std::cout << "isConnectionActive() " << m_connection.active() << std::endl;
  }

  bool AmqpClient::connectionActive()
  {
    std::cout << "connectionActive() " << m_connection.active() << std::endl;

    return m_connectionActive;
    //return m_connection.active();
  }

  proton::connection AmqpClient::connection() const
  {
    return m_connection;
  }

} // namespace fty::messagebus::amqp
