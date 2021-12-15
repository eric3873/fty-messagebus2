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

  class EtnMessageBus final : public fty::messagebus::MessageBus
  {
  public:
    EtnMessageBus(const fty::messagebus::ClientName& clientName = fty::messagebus::utils::getClientId("EtnMessageBus"))
      : m_clientName(clientName){};

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

    [[nodiscard]] fty::Expected<void> connect(BusType busType) noexcept;
  };

} // namespace etn::messagebus
