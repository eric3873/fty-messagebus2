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

#include "AmqpReceiver.h"
#include "AmqpClient.h"
#include "MsgBusAmqpUtils.h"

#include <fty_log.h>

#include <qpid/messaging/Address.h>
#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Sender.h>
#include <qpid/messaging/Session.h>

namespace fty::messagebus2::amqp {
using namespace fty::messagebus2;
using namespace qpid::messaging;

using MessageListener = fty::messagebus2::MessageListener;

AmqpClient::AmqpClient(const ClientName& clientName, const Endpoint& url) :
    m_clientName(clientName),
    m_url(url)
{
    m_connection = std::make_shared<Connection>(url);
    m_connection->setOption("reconnect", true);      // Deactivated by default
    m_connection->setOption("reconnect_limit", -1);  // Try to reconnect indefinitely

}

AmqpClient::~AmqpClient()
{
    close();
}

ComState AmqpClient::connect()
{
    try {
        m_closed = false;
        m_connection->open();
        if (!m_connection->isOpen()) {
            return ComState::ConnectFailed;
        }
        auto session = m_connection->createSession(DEFAULT_SESSION);
        return ComState::Connected;
    }
    catch(const std::exception& error) {
        logError("Client connection error: {}", error.what());
        m_connection->close();
        m_closed = true;
        return ComState::ConnectFailed;
    }
}

bool AmqpClient::isConnected() {
    // Test if connected
    return (m_connection && m_connection->isOpen());
}

DeliveryState AmqpClient::send(const qpid::messaging::Message& msg)
{
    auto deliveryState = DeliveryState::Accepted;

    auto address = sanitizeAddress(getAddress(msg));
    Sender sender = m_connection->getSession(DEFAULT_SESSION).createSender(address);
    sender.send(msg);
    sender.close();
    return deliveryState;
}

DeliveryState AmqpClient::receive(const Address& addressIn, MessageListener messageListener, const std::string& filter)
{
    auto deliveryState = DeliveryState::Rejected;

    logDebug("Receive address (address:{}, filter:{})", addressIn, filter);

    // Test address
    if (addressIn.empty()) {
        logError("Receive address: Address is required");
        return deliveryState;
    }

    // Test callback
    if (!messageListener) {
        logError("Receive address: Callback not filled");
        return deliveryState;
    }

    auto address = sanitizeAddress(addressIn);

    // Then search if a receiver with this address exist
    AmqpReceiverPointer receiver = nullptr;
    std::lock_guard<std::mutex> lock(m_lock);
    for (auto it_receiver = m_receivers.begin(); it_receiver != m_receivers.end(); it_receiver ++) {
        if ((*it_receiver)->getAddress() == address) {
           receiver = *it_receiver;
           break;
        }
    }
    // If address not found, create a new receiver on this address
    if (!receiver) {
        receiver = std::make_shared<AmqpReceiver>(this, address);
        if (receiver && receiver->init(filter, messageListener)) {
            logDebug("receive: add new receiver (name:{}, address:{}, filter:{})", receiver->getName(), address, filter);
            m_receivers.push_back(receiver);
            deliveryState = DeliveryState::Accepted;
        }
    }
    else {
        // Address is present, just add new filter (if not exist)
        if (receiver->setSubscription(filter, messageListener)) {
            logDebug("receive: add new filter (address:{}, filter:{})", address, filter);
            deliveryState = DeliveryState::Accepted;
        }
        else {
            logError("Receive error: receiver with filter exist yet (address:{}, filter:{})", address, filter);
        }
    }
    return deliveryState;
}

DeliveryState AmqpClient::unreceive(const Address& addressIn, const std::string& filter)
{
    auto deliveryState = DeliveryState::Rejected;

    logDebug("Unreceive address (address:{}, filter:{})", addressIn, filter);

    // First, test address
    if (addressIn.empty()) {
        logError("Unreceive address: Address is required");
        return deliveryState;
    }

    auto address = sanitizeAddress(addressIn);

    // Then search address with filter
    std::lock_guard<std::mutex> lock(m_lock);
    for (auto it_receiver = m_receivers.begin(); it_receiver != m_receivers.end(); it_receiver ++) {
        if ((*it_receiver)->getAddress() == address) {
            auto receiver = *it_receiver;
            if (receiver->unsetSubscription(filter)) {
                // Receiver can be closed if no more filter on it
                if (receiver->getSubscriptionsNumber() == 0) {
                    if (!(*it_receiver)->waitClose()) {
                        logWarn("Unreceive timeout reached (name: {})", (*it_receiver)->getName());
                    }
                    m_receivers.erase(it_receiver);
                }
                logDebug("unreceive: remove filter (address:{}, filter:{})", address, filter);
                deliveryState = DeliveryState::Accepted;
            }
            else {
                logError("Error when unset subscription (address:{} filter:{})", address, filter);
            }
            break;
        }
    }
    return deliveryState;
}

void AmqpClient::close()
{
    m_closed = true;

    // Wait for all the receivers to close
    std::lock_guard<std::mutex> lock(m_lock);
    for (auto it_receiver = m_receivers.begin(); it_receiver != m_receivers.end(); it_receiver ++) {
        // Close message receiver thread
        if (!(*it_receiver)->waitClose()) {
            logWarn("Close receiver timeout reached (name: {})", (*it_receiver)->getName());
        }
    }
}

} // namespace fty::messagebus2::amqp
