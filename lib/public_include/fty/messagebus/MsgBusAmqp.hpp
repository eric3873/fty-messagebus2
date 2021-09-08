/*  =========================================================================
    MsgBusAmqp.hpp - class description

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

#include <fty/messagebus/MsgBusWrapper.hpp>
#include <fty/messagebus/amqp/MsgBusAmqp.hpp>
#include <fty/messagebus/utils/MsgBusHelper.hpp>

namespace fty::messagebus
{
  class MsgBusAmqp : public MsgBusWrapper<amqp::MessageBusAmqp, amqp::AmqpMessage, amqp::UserData>
  {
  public:
    MsgBusAmqp(const ClientName& clientName = utils::getClientId("MsgBusAmqp"), const Endpoint& endpoint = fty::messagebus::amqp::DEFAULT_AMQP_END_POINT);
    ~MsgBusAmqp() = default;

    MsgBusAmqp(MsgBusAmqp && other) = default;
    MsgBusAmqp& operator=(MsgBusAmqp&& other) = delete;
    MsgBusAmqp(const MsgBusAmqp& other) = default;
    MsgBusAmqp& operator=(const MsgBusAmqp& other) = delete;

    DeliveryState subscribe(const std::string& topic, MessageListener<amqp::AmqpMessage> messageListener) override;
    DeliveryState unsubscribe(const std::string& topic) override;
    DeliveryState publish(const std::string& topic, const amqp::UserData& msg) override;

    DeliveryState sendRequest(const std::string& requestQueue, const amqp::UserData& request, MessageListener<amqp::AmqpMessage> messageListener) override;
    Opt<amqp::AmqpMessage> sendRequest(const std::string& requestQueue, const amqp::UserData& request, int timeOut) override;
    DeliveryState registerRequestListener(const std::string& requestQueue, MessageListener<amqp::AmqpMessage> messageListener) override;
    DeliveryState sendRequestReply(const amqp::AmqpMessage& inputRequest, const amqp::UserData& response) override;

    amqp::AmqpMessage buildMessage(const std::string& queue, const amqp::UserData& msg) override;


  private:
    //amqp::AmqpMessage buildMessage(const std::string& queue, const amqp::UserData& msg) override;
  };
} // namespace fty::messagebus
