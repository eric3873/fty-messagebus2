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

#pragma once

#include <fty/messagebus2/MessageBus.h>
#include <fty/messagebus2/Promise.h>

namespace fty::messagebus2::amqp::msg {

using MessageListener      = fty::messagebus2::MessageListener;
using SubScriptionListener = std::map<std::string, MessageListener>;

// Timeout for waiting close messaging thread
static auto constexpr TIMEOUT_MS = 5000;

class AmqpClient;

class AmqpReceiver
{
public:
    AmqpReceiver(
        fty::messagebus2::amqp::msg::AmqpClient *client,
        const std::string& name,
        const std::string& address,
        const std::string& filter,
        const MessageListener& messageListener);
    ~AmqpReceiver();

    bool waitClose();
    const std::string getName() { return m_name; };
    const Address getAddress() { return m_address; };

    ulong getSubscriptionsNumber();
    // Get the subscription according the input filter
    MessageListener getSubscription(const std::string& filter);
    // Add filter for subscription
    bool setSubscription(const std::string& filter, MessageListener messageListener);
    // Remove filter for subscription
    bool unsetSubscription(const std::string& filter);

protected:
    // Thread to manage message reception
    void manageMessage();

private:
    // Client reference
    AmqpClient*          m_client;
    // Internal name of the qpid receiver
    std::string          m_name;
    // Address of receiver
    std::string          m_address;
    // List of message subscriptions
    SubScriptionListener m_subscriptions;

    // Mutex for subscription list
    std::mutex           m_lock;

    // Flag and promise used for closing the messaging thread
    std::atomic<bool>               m_closed;
    fty::messagebus2::Promise<void> m_promiseClose;
};

} // namespace fty::messagebus2::amqp::msg
