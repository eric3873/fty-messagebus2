/*  =========================================================================
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

#include <functional>
#include <memory>
#include <optional>
#include <string>

#include <fty/expected.h>
#include <fty/messagebus/Message.h>

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
    MessageBus(MessageBus&&) noexcept = default;

    /// Connect to the MessageBus
    /// @return Success or Com Error
    virtual [[nodiscard]] fty::Expected<void> connect() noexcept = 0 ;

    /// Send a message
    /// @param msg the message object to send
    /// @return Success or Delivery error
    virtual [[nodiscard]] fty::Expected<void> send(const Message& msg) noexcept = 0 ;


    /// Register a listener to a address using function
    /// @param queue the address to subscribe
    /// @param func the function to subscribe
    /// @return Success or error
    virtual [[nodiscard]] fty::Expected<void> subscribe(const std::string& address, MessageListener && func) noexcept = 0 ;

    /// Unsubscribe from a queue
    /// @param queue the address to unsubscribe
    /// @return Success or error
    virtual [[nodiscard]] fty::Expected<void> unsubscribe(const std::string& address) noexcept = 0 ;

    /// Register a listener to a address using class
    /// @example
    ///     bus.subsribe("address", &MyCls::onMessage, this);
    /// @param address the address to subscribe
    /// @param fnc the member function to subscribe
    /// @param cls class instance
    /// @return Success or error
    template <typename Func, typename Cls>
    [[nodiscard]] fty::Expected<void> subscribe(const std::string& address, Func&& fnc, Cls* cls) noexcept
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

  constexpr const char* COM_STATE_LOST = "COM_STATE_LOST";
  constexpr const char* COM_STATE_NO_CONTACT = "COM_STATE_NO_CONTACT";
  constexpr const char* COM_STATE_CONNECT_FAILED = "COM_STATE_CONNECT_FAILED";

  constexpr const char* DELIVERY_STATE_REJECTED = "DELIVERY_STATE_REJECTED";
  constexpr const char* DELIVERY_STATE_TIMEOUT = "DELIVERY_STATE_TIMEOUT";
  constexpr const char* DELIVERY_STATE_NOT_SUPPORTED = "DELIVERY_STATE_NOT_SUPPORTED";
  constexpr const char* DELIVERY_STATE_PENDING = "DELIVERY_STATE_PENDING";
  constexpr const char* DELIVERY_STATE_BUSY = "DELIVERY_STATE_BUSY";
  constexpr const char* DELIVERY_STATE_ABORTED = "DELIVERY_STATE_ABORTED";
  constexpr const char* DELIVERY_STATE_UNAVAILABLE = "DELIVERY_STATE_UNAVAILABLE";

} // namespace fty::messagebus
