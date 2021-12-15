/*  =========================================================================
    Copyright (C) 2014 - 2022 Eaton

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
#include "etn/messagebus/EtnMessage.h"

#include <fty/messagebus/utils.h>

namespace etn::messagebus
{
  using namespace fty::messagebus;

  static constexpr auto ETN_TOPIC_PREFIX = "/etn/t/";
  static const std::string QUEUE_PREFIX = "queue://";
  static const std::string ETN_QUEUE_PREFIX = QUEUE_PREFIX + "etn.q.";
  static const std::string ETN_QUEUE_REQUEST = ETN_QUEUE_PREFIX + "request.";
  static const std::string ETN_QUEUE_REPLY = ETN_QUEUE_PREFIX + "reply.";

  Address EtnMessage::buildAddress(const Address& address, const AddressType& addressType)
  {
    Address etnAddress;
    switch (addressType)
    {
      case AddressType::TOPIC:
        etnAddress = ETN_TOPIC_PREFIX + address;
        break;
      case AddressType::QUEUE:
        etnAddress = ETN_QUEUE_PREFIX + address;
        break;
      case AddressType::REQUEST_QUEUE:
        etnAddress = ETN_QUEUE_REQUEST + address;
        break;
      case AddressType::REPLY_QUEUE:
        etnAddress = ETN_QUEUE_REPLY + address;
        break;
      default:
        etnAddress = address;
        break;
    }
    return etnAddress;
  }

  BusType EtnMessage::getBusType(const Address& address)
  {
    BusType busType = BusType::MQTT;
    if (address.find(QUEUE_PREFIX) != std::string::npos)
    {
      busType = BusType::AMQP;
    }
    return busType;
  }

} //namespace etn::messagebus
