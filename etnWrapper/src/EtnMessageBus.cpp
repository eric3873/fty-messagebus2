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
#include "etn/messagebus/EtnMessageBus.h"
#include "etn/messagebus/EtnMessage.h"

#include <fty/messagebus/amqp/MessageBusAmqp.h>

namespace etn::messagebus
{
  using namespace fty::messagebus;
  using namespace fty::messagebus::amqp;
  using namespace fty::messagebus::mqtt;

  static const std::string INDENTITY("etnMessageBusAmqpMqtt");

  fty::Expected<void> EtnMessageBus::send(const Message& msg) noexcept
  {
    fty::Expected<void> result;
    switch (getBusType(msg.to()))
    {
      case BusType::AMQP:
        // For request/reply pattern prefer amqp message bus implementation
        result = connect(BusType::AMQP);
        if (result)
        {
          result = m_busAmqp->send(msg);
        }
        break;
      default:
        // Otherwise (pub/sub pattern) prefer mqtt message bus implementation
        result = connect(BusType::MQTT);
        if (result)
        {
          result = m_busMqtt->send(msg);
        }
        break;
    }
    return result;
  }

  fty::Expected<void> EtnMessageBus::receive(const Address& address, MessageListener&& func, const std::string& filter) noexcept
  {
    fty::Expected<void> result;
    switch (getBusType(address))
    {
      case BusType::AMQP:
        result = connect(BusType::AMQP);
        if (result)
        {
          result = m_busAmqp->receive(address, std::move(func), filter);
        }
        break;
      default:
        result = connect(BusType::MQTT);
        if (result)
        {
          result = m_busMqtt->receive(address, std::move(func), filter);
        }
        break;
    }
    return result;
  }

  fty::Expected<void> EtnMessageBus::unreceive(const Address& address) noexcept
  {
    fty::Expected<void> result;
    switch (getBusType(address))
    {
      case BusType::AMQP:
        result = connect(BusType::AMQP);
        if (result)
        {
          result = m_busAmqp->unreceive(address);
        }
        break;
      default:
        result = connect(BusType::MQTT);
        if (result)
        {
          result = m_busMqtt->unreceive(address);
        }
        break;
    }
    return result;
  }

  fty::Expected<Message> EtnMessageBus::request(const Message& msg, int timeOut) noexcept
  {
    //fty::Expected<Message> result;
    if (connect(BusType::AMQP))
    {
      return m_busAmqp->request(msg, timeOut);
      // result = m_busAmqp->request(msg, timeOut);
      // return result;
    }
    return {};//result;
  }

  const std::string& EtnMessageBus::clientName() const noexcept
  {
    return m_clientName;
  }

  const std::string& EtnMessageBus::identity() const noexcept
  {
    return INDENTITY;
  }

  fty::Expected<void> EtnMessageBus::connect(BusType busType) noexcept
  {
    fty::Expected<void> result;
    switch (busType)
    {
      case BusType::MQTT:
        if (!m_busMqtt)
        {
          m_busMqtt = std::make_shared<MessageBusMqtt>(m_clientName, fty::messagebus::mqtt::DEFAULT_ENDPOINT);
          result = m_busMqtt->connect();
        }
        break;
      default:
        if (!m_busAmqp)
        {
          m_busAmqp = std::make_shared<MessageBusAmqp>(m_clientName, fty::messagebus::amqp::DEFAULT_ENDPOINT);
          result = m_busAmqp->connect();
        }
        break;
    }
    return result;
  }

  BusType EtnMessageBus::getBusType(const Address& address) noexcept
  {
    BusType busType = BusType::MQTT;
    if (address.find(QUEUE_PREFIX) != std::string::npos)
    {
      busType = BusType::AMQP;
    }
    return busType;
  }

} //namespace etn::messagebus
