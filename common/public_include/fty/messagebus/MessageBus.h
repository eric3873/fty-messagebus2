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

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <fty/expected.h>
#include "fty/messagebus/Message.h"

namespace fty::messagebus
{
  using ClientName = std::string;
  using Endpoint = std::string;
  using Identity = std::string;

  using MessageListener = std::function<void(const Message&)>;

  class MessageBus
  {
  public:
    MessageBus() noexcept = default;
    virtual ~MessageBus() = default;
    MessageBus(const MessageBus&) = delete;
    MessageBus(MessageBus&&) noexcept = delete;

    /// Connect to the MessageBus
    /// @return Success or Com Error
    virtual [[nodiscard]] fty::Expected<void> connect() noexcept = 0 ;

    /// Send a message
    /// @param msg the message object to send
    /// @return Success or Delivery error
    virtual [[nodiscard]] fty::Expected<void> send(const Message& msg) noexcept = 0 ;


    /// Register a listener to a address using function
    /// @param address the address to receive
    /// @param func the function to receive
    /// @param filter constraint the receiver with a filter
    /// @return Success or error
    virtual [[nodiscard]] fty::Expected<void> receive(const Address& address, MessageListener&& func, const std::string& filter = {}) noexcept = 0 ;

    /// Unsubscribe from a address
    /// @param address the address to unsubscribe
    /// @return Success or error
    virtual [[nodiscard]] fty::Expected<void> unreceive(const Address& address) noexcept = 0 ;

    /// Register a listener to a address using class
    /// @example
    ///     bus.subsribe("address", &MyCls::onMessage, this);
    /// @param address the address to receive
    /// @param fnc the member function to receive
    /// @param cls class instance
    /// @return Success or error
    template <typename Func, typename Cls>
    [[nodiscard]] fty::Expected<void> receive(const Address& address, Func&& fnc, Cls* cls) noexcept
    {
        return registerListener(address, [f = std::move(fnc), c = cls](const Message& msg) -> void {
            std::invoke(f, *c, Message(msg));
        });
    }

    /// Sends message to the queue and wait to receive response
    /// @param msg the message to send
    /// @param timeOut the timeout for the request
    /// @return Response message or Delivery error
    virtual [[nodiscard]] fty::Expected<Message> request(const Message& msg, int timeOut) noexcept = 0 ;

    /// Get the client name
    /// @return Client name
    virtual [[nodiscard]] const ClientName & clientName() const noexcept = 0 ;

    /// Get MessageBus Identity
    /// @return MessageBus Identity
    virtual [[nodiscard]] const Identity & identity() const noexcept = 0 ;

  };

} // namespace fty::messagebus
