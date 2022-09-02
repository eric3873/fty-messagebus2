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
#include "fty/messagebus2/amqp/MessageBusAmqp.h"
#include "AmqpClient.h"
#include "MsgBusAmqpUtils.h"

#include <fty/messagebus2/Promise.h>
#include <fty/expected.h>
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty_log.h>

namespace fty::messagebus2::amqp::msg {

using namespace fty::messagebus2;

MessageBusAmqp::MessageBusAmqp(const ClientName& clientName, const Endpoint& endpoint) : MessageBus(),
    m_clientName(clientName),
    m_endpoint(endpoint),
    m_clientPtr(std::make_shared<AmqpClient>(clientName, endpoint))
{
}

MessageBusAmqp::~MessageBusAmqp()
{
}

fty::Expected<void, ComState> MessageBusAmqp::connect() noexcept
{
    if (!m_clientPtr) {
        logError("Client not initialized for endpoint");
        return fty::unexpected(ComState::ConnectFailed);
    }

    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    try {
        auto connection = m_clientPtr->connect();
        if (connection != ComState::Connected) {
            return fty::unexpected(connection);
        }
    } catch (const std::exception& e) {
        logError("Unexpected error: {}", e.what());
        return fty::unexpected(ComState::ConnectFailed);
    }
    return {};
}

bool MessageBusAmqp::isServiceAvailable()
{
    return (m_clientPtr && m_clientPtr->isConnected());
}

fty::Expected<void, DeliveryState> MessageBusAmqp::receive(
    const Address& address, MessageListener&& messageListener, const std::string& filter) noexcept
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    auto received = m_clientPtr->receive(address, messageListener, filter);
    if (received != DeliveryState::Accepted) {
        logError("Message receive (Rejected)");
        return fty::unexpected(DeliveryState::Rejected);
    }
    return {};
}

fty::Expected<void, DeliveryState> MessageBusAmqp::unreceive(const Address& address, const std::string& filter) noexcept
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }
    auto res = m_clientPtr->unreceive(address, filter);
    if (res != DeliveryState::Accepted) {
        return fty::unexpected(DeliveryState::Rejected);
    }
    return {};
}

fty::Expected<void, DeliveryState> MessageBusAmqp::send(const Message& message) noexcept
{

    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    logDebug("Sending message ...");
    qpid::messaging::Message msgToSend = getAmqpMessage(message);

    auto msgSent = m_clientPtr->send(msgToSend);
    if (msgSent != DeliveryState::Accepted) {
        logError("Message sent (Rejected)");
        return fty::unexpected(msgSent);
    }

    logDebug("Message sent (Accepted)");

    return {};
}

fty::Expected<Message, DeliveryState> MessageBusAmqp::request(const Message& message, int timeoutInSeconds) noexcept
{
    try {
        if (!isServiceAvailable()) {
            logDebug("Service not available");
            return fty::unexpected(DeliveryState::Unavailable);
        }

        // Sanity check
        if (!message.isValidMessage()) {
            return fty::unexpected(DeliveryState::Rejected);
        }
        if (!message.needReply()) {
            return fty::unexpected(DeliveryState::Rejected);
        }

        logDebug("Synchronous request and checking answer until {} second(s)...", timeoutInSeconds);
        qpid::messaging::Message msgToSend = getAmqpMessage(message);

        logDebug("msg={}", qpidMsgtoString(msgToSend));

        // Promise and future to check if the answer arrive constraint by timeout.
        Promise<Message> promiseSyncRequest(*this, msgToSend.getReplyTo().str(), msgToSend.getCorrelationId());

        bool msgArrived = false;

        auto msgReceived = receive(
            msgToSend.getReplyTo().str(),
            std::move(std::bind(&Promise<Message>::setValue, &promiseSyncRequest, std::placeholders::_1)),
            msgToSend.getCorrelationId());
        if (!msgReceived) {
            return fty::unexpected(DeliveryState::Aborted);
        }

        // Send message
        auto msgSent = send(message);
        if (!msgSent) {
            return fty::unexpected(DeliveryState::Aborted);
        }

        // Wait response
        if (promiseSyncRequest.waitFor(timeoutInSeconds * 1000)) {
            msgArrived = true;
        }

        if (!msgArrived) {
            logError("No message arrived within {} seconds!", timeoutInSeconds);
            return fty::unexpected(DeliveryState::Timeout);
        }

        auto value = promiseSyncRequest.getValue();
        if (value) {
            return *value;
        }
        return fty::unexpected(DeliveryState::Aborted);
    } catch (const std::exception& e) {
        logError("Exception in synchronous request: {}", e.what());
        return fty::unexpected(DeliveryState::Aborted);
    }
}

const std::string& MessageBusAmqp::clientName() const noexcept
{
    return m_clientName;
}

static const std::string g_identity(BUS_IDENTITY);

const std::string& MessageBusAmqp::identity() const noexcept
{
    return g_identity;
}

} // namespace fty::messagebus2::amqp::msg

