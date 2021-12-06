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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>

#include <cstdlib>
#include <cxxabi.h>
#include <iostream>
#include <regex>

#include <mutex>
#include <thread>

namespace fty::messagebus
{
  static const std::string QUEUE = "queue:/";
  static const std::string TOPIC = "topic:/";

  template <typename T>
  static const std::string typeName()
  {
    int status;
    std::string tname = typeid(T).name();
    char* demangled_name = abi::__cxa_demangle(tname.c_str(), NULL, NULL, &status);
    if (status == 0)
    {
      tname = demangled_name;
      std::free(demangled_name);
    }
    return tname;
  }

  //template <typename T, typename... Args>
  template <typename T>
  struct MsgBusFixture
  {
    MsgBusFixture(const ClientName& clientName = {}, const Endpoint& endpoint = {})
      : m_bus(clientName, endpoint.empty() ? getEndpoint() : endpoint)
    {
    }

    bool isAmqpMsgBus()
    {
      return (typeName<T>().find("amqp") != std::string::npos);
    }

    const std::string getEndpoint()
    {
      return isAmqpMsgBus() ? amqp::DEFAULT_ENDPOINT : mqtt::DEFAULT_ENDPOINT;
    }

    const std::string getIdentity()
    {
      return isAmqpMsgBus() ? amqp::BUS_IDENTITY : mqtt::BUS_IDENTITY;
    }

    const std::string getAddress(const std::string address, const std::string addressType = QUEUE)
    {
      return isAmqpMsgBus() ? addressType + address : std::regex_replace(address, std::regex("\\."), "/");
    }

    T m_bus;
  };

} // namespace fty::messagebus
