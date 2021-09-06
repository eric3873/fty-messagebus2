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

#include "fty/messagebus/amqp/MsgBusAmqpClient.hpp"
#include <fty/messagebus/IMessageBus.hpp>
#include <fty/messagebus/MsgBusStatus.hpp>
#include <fty/messagebus/amqp/MsgBusAmqpMessage.hpp>
#include <fty/messagebus/amqp/Client.hpp>

#include <thread>
#include <vector>
#include <map>

namespace fty::messagebus::amqp
{
  // Default mqtt end point
  static auto constexpr DEFAULT_AMQP_END_POINT{"amqp://127.0.0.1:5672"};

  using Container = proton::container;
  using ClientPointer = std::shared_ptr<Client>;
  using ContainerPointer = std::shared_ptr<Container>;

  using MessageListener = fty::messagebus::MessageListener<AmqpMessage>;

  class MessageBusAmqp final : public IMessageBus<AmqpMessage>
  {
  public:
    MessageBusAmqp() = delete;

    MessageBusAmqp(const std::string& clientName, const std::string& endpoint)
      : m_clientName(clientName)
      , m_endPoint(endpoint){};

    ~MessageBusAmqp() override;

    [[nodiscard]] fty::messagebus::ComState connect() override;

    // Pub/Sub pattern
    DeliveryState publish(const std::string& topic, const AmqpMessage& message) override;
    DeliveryState subscribe(const std::string& topic, MessageListener messageListener) override;
    DeliveryState unsubscribe(const std::string& topic, MessageListener messageListener = {}) override;

    // Req/Rep pattern
    DeliveryState sendRequest(const std::string& requestQueue, const AmqpMessage& message) override;
    DeliveryState sendRequest(const std::string& requestQueue, const AmqpMessage& message, MessageListener messageListener) override;
    DeliveryState sendReply(const std::string& replyQueue, const AmqpMessage& message) override;
    DeliveryState receive(const std::string& queue, MessageListener messageListener) override;

    // Sync queue
    Opt<AmqpMessage> request(const std::string& requestQueue, const AmqpMessage& message, int receiveTimeOut) override;

  private:
    std::string m_clientName{};
    std::string m_endPoint{};


    ContainerPointer m_container;
    ClientPointer m_client;

    //Container container_  = Container();
    //std::vector<std::thread::native_handle_type> m_containerThreads;
    std::map<std::string, pthread_t> m_containerThreads;
    //client m_client = client();


    //std::thread m_containerThreads;

    // Call back
    //CallBack m_cb;

    bool isServiceAvailable();
  };
} // namespace fty::messagebus::amqp
