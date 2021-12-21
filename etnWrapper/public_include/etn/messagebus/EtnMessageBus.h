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

#include "etn/messagebus/EtnMessage.h"

#include <fty/expected.h>
#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>
#include <fty/messagebus/utils.h>

#include <functional>
#include <memory>
#include <optional>
#include <string>

namespace etn::messagebus
{
  // All bus type implementation
  enum class BusType
  {
    AMQP,
    MQTT
  };

  // Endpoint broker for amqp and mqtt
  struct EndpointBroker
  {

  public:

    // EndpointBroker constructor
    /// @param amqpEndpoint Amqp broker endpoint
    /// @param mqttEndpoint Mqtt broker endpoint
    EndpointBroker(const fty::messagebus::Endpoint& amqpEndpoint = fty::messagebus::amqp::DEFAULT_ENDPOINT, const fty::messagebus::Endpoint& mqttEndpoint = fty::messagebus::mqtt::DEFAULT_ENDPOINT)
      : m_amqpEndpoint(amqpEndpoint)
      , m_mqttEndpoint(mqttEndpoint)
    {
    }

    void amqpEndpoint(const fty::messagebus::Endpoint& endpoint)
    {
      m_amqpEndpoint = endpoint;
    }

    fty::messagebus::Endpoint amqpEndpoint() const
    {
      return m_amqpEndpoint;
    }

    void mqttEndpoint(const fty::messagebus::Endpoint& endpoint)
    {
      m_mqttEndpoint = endpoint;
    }

    fty::messagebus::Endpoint mqttEndpoint() const
    {
      return m_mqttEndpoint;
    }

  private:
    fty::messagebus::Endpoint m_amqpEndpoint;
    fty::messagebus::Endpoint m_mqttEndpoint;
  };

  class EtnMessageBus final : public fty::messagebus::MessageBus
  {
  public:

    // EtnMessageBus constructor
    /// @param clientName Client name if not set a default one is built
    /// @param brokerAddress Endpoint brokers for amqp and mqtt.
    EtnMessageBus(const fty::messagebus::ClientName& clientName = fty::messagebus::utils::getClientId("EtnMessageBus"), const EndpointBroker& brokerAddress = {})
      : m_clientName(clientName)
      , m_endpointBroker(brokerAddress){};

    ~EtnMessageBus() = default;

    EtnMessageBus(EtnMessageBus&& other) = default;
    EtnMessageBus& operator=(EtnMessageBus&& other) = delete;
    EtnMessageBus(const EtnMessageBus& other) = default;
    EtnMessageBus& operator=(const EtnMessageBus& other) = delete;

    // /// Connect to the MessageBus
    // /// @return Success or Com Error
    virtual [[nodiscard]] fty::Expected<void> connect() noexcept override;

    /// Send a message
    /// @param msg the message object to send
    /// @return Success or Delivery error
    [[nodiscard]] fty::Expected<void> send(const fty::messagebus::Message& msg) noexcept override;

    /// Register a listener to a address using function
    /// @param address the address to receive
    /// @param func the function to receive
    /// @param filter constraint the receiver with a filter
    /// @return Success or error
    [[nodiscard]] fty::Expected<void> receive(const fty::messagebus::Address& address, fty::messagebus::MessageListener&& func, const std::string& filter = {}) noexcept override;

    // /// Register a listener to a address using class
    // /// @example
    // ///     bus.subsribe("address", &MyCls::onMessage, this);
    // /// @param address the address to receive
    // /// @param fnc the member function to receive
    // /// @param cls class instance
    // /// @return Success or error
    // template <typename Func, typename Cls>
    // [[nodiscard]] fty::Expected<void> receive(const std::string& address, Func&& fnc, Cls* cls) noexcept
    // {
    //   return registerListener(address, [f = std::move(fnc), c = cls](const Message& msg) -> void {
    //     std::invoke(f, *c, Message(msg));
    //   });
    // }

    /// Unsubscribe from a address
    /// @param address the address to unsubscribe
    /// @return Success or error
    [[nodiscard]] fty::Expected<void> unreceive(const fty::messagebus::Address& address) noexcept override;

    /// Sends message to the queue and wait to receive response
    /// @param msg the message to send
    /// @param timeOut the timeout for the request
    /// @return Response message or Delivery error
    [[nodiscard]] fty::Expected<fty::messagebus::Message> request(const fty::messagebus::Message& msg, int timeOut) noexcept override;

    // /// Get the client name
    // /// @return Client name
    [[nodiscard]] const fty::messagebus::ClientName& clientName() const noexcept override;

    // /// Get MessageBus Identity
    // /// @return MessageBus Identity
    [[nodiscard]] const fty::messagebus::Identity& identity() const noexcept override;

  private:
    fty::messagebus::ClientName m_clientName{};
    std::shared_ptr<fty::messagebus::amqp::MessageBusAmqp> m_busAmqp;
    std::shared_ptr<fty::messagebus::mqtt::MessageBusMqtt> m_busMqtt;

    EndpointBroker m_endpointBroker;

    /// Connect to the right MessageBus, depending of bustype (AMQP|MQTT)
    /// @return Success or Com Error
    [[nodiscard]] fty::Expected<void> connect(BusType busType) noexcept;

    /// Get the busType, depending of the address content (AMQP|MQTT)
    /// @return The busType
    static BusType getBusType(const fty::messagebus::Address& address) noexcept;
  };

} // namespace etn::messagebus
