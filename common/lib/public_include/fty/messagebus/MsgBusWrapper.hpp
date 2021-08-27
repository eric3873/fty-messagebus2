/*  =========================================================================
    PublishSubscribe - class description

    Copyright (C) 2014 - 2020 Eaton

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

#include "fty/messagebus/IMessageBus.hpp"
#include "fty/messagebus/MsgBusStatus.hpp"
#include <fty/messagebus/MsgBusException.hpp>
#include <fty/messagebus/MsgBusFactory.hpp>

#include <fty_log.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace fty::messagebus
{
  using ClientName = std::string;
  using Endpoint = std::string;
  using Identify = std::string;

  template <typename MessageBusType,
            typename MessageType,
            typename UserDataType>
  class MsgBusWrapper
  {
  public:
    MsgBusWrapper() = default;
    MsgBusWrapper(const ClientName& clientName, const Endpoint& endpoint, const Identify& identify)
      : m_clientName(clientName)
      , m_endPoint(endpoint)
      , m_identify(identify)
      , m_msgBus{fty::messagebus::MessageBusFactory<MessageBusType>::createMsgBus(clientName, endpoint)}
    {
      auto state = m_msgBus->connect();
      if (state != fty::messagebus::COM_STATE_OK)
      {
        std::string errorMsg("Messagebus server connection error on: ");
        errorMsg += endPoint();
        throw MessageBusException(errorMsg);
      }
    };

    virtual ~MsgBusWrapper() = default;

    ClientName clientName() const
    {
      return m_clientName;
    };

    Endpoint endPoint() const
    {
      return m_endPoint;
    };

    Identify identify() const
    {
      return m_identify;
    };

    // Publish/Subcribe  pattern
    virtual DeliveryState subscribe(const std::string& topic, MessageListener<MessageType> messageListener) = 0;
    virtual DeliveryState unsubscribe(const std::string& topic) = 0;
    virtual DeliveryState publish(const std::string& topic, const UserDataType& msg) = 0;

    // Request/Reply  pattern
    virtual DeliveryState sendRequest(const std::string& requestQueue, const UserDataType& msg, MessageListener<MessageType> messageListener) = 0;
    virtual Opt<MessageType> sendRequest(const std::string& requestQueue, const UserDataType& msg, int timeOut) = 0;
    virtual DeliveryState registerRequestListener(const std::string& requestQueue, MessageListener<MessageType> messageListener) = 0;
    virtual DeliveryState sendRequestReply(const MessageType& inputRequest, const UserDataType& response) = 0;

  private:
    ClientName m_clientName{};
    Endpoint m_endPoint{};
    Identify m_identify{};


  protected:
    std::unique_ptr<MessageBusType> m_msgBus;
    virtual MessageType buildMessage(const std::string& queue, const UserDataType& msg) = 0;
  };

} // namespace fty::messagebus
