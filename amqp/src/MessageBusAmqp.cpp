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
#include "fty/messagebus2/amqp/MessageBusAmqp.h"
#include "AmqpClient.h"
#include <fty/expected.h>
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty_log.h>
#include <memory>

namespace fty::messagebus2::amqp {

using namespace fty::messagebus2;
using proton::receiver_options;
using proton::source_options;

MessageBusAmqp::MessageBusAmqp(const ClientName& clientName, const Endpoint& endpoint) : MessageBus(), 
    m_clientName(clientName),
    m_endpoint  (endpoint),
    m_clientPtr (std::make_shared<AmqpClient>(endpoint)) 
{
}

fty::Expected<void, ComState> MessageBusAmqp::connect() noexcept
{
    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    try {
        std::thread thrdSender([=]() {
            proton::container(*m_clientPtr).run();
        });
        thrdSender.detach();

        auto connection = m_clientPtr->connected();
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
    return (m_clientPtr->isConnected());
}

fty::Expected<void, DeliveryState> MessageBusAmqp::receive(
    const Address& address, MessageListener&& messageListener, const std::string& filter) noexcept
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    if (!m_clientPtr) {
        logError("Client not initialized for endpoint");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    auto received = m_clientPtr->receive(address, messageListener, filter);
    if (received != DeliveryState::Accepted) {
        logError("Message receive (Rejected)");
        return fty::unexpected(DeliveryState::Rejected);
    }
    return {};
}

fty::Expected<void, DeliveryState> MessageBusAmqp::unreceive(const Address& address) noexcept
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }
    auto res = m_clientPtr->unreceive(address);
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
    proton::message msgToSend = getAmqpMessage(message);

    if (!m_clientPtr) {
        logError("Client not initialised for endpoint");
        return fty::unexpected(DeliveryState::Unavailable);
    }

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
        proton::message msgToSend = getAmqpMessage(message);

        // Promise and future to check if the answer arrive constraint by timeout.
        auto promiseSyncRequest = std::promise<Message>();

        Message reply;
        bool msgArrived = false;
        MessageListener&& syncMessageListener = [&](const Message& replyMessage) {
            // TODO: To rework with common promise class (protection against promise already satisfied)
            // CAUTION: When Receive several messages, try to set a future already used
            try {
                promiseSyncRequest.set_value(replyMessage);
            }
            catch (const std::exception& e) {
               logWarn("promiseSyncRequest error: {}", e.what());
            }
        };

        auto msgReceived = receive(msgToSend.reply_to(), std::move(syncMessageListener), proton::to_string(msgToSend.correlation_id()));
        if (!msgReceived) {
            return fty::unexpected(DeliveryState::Aborted);
        }

        auto msgSent = send(message);
        if (!msgSent) {
            auto unreceived = unreceive(msgToSend.reply_to());
            if (!unreceived) {
                logWarn("Issue on unreceive");
            }
            return fty::unexpected(DeliveryState::Aborted);
        }

        auto futureSynRequest = promiseSyncRequest.get_future();
        if (futureSynRequest.wait_for(std::chrono::seconds(timeoutInSeconds)) != std::future_status::timeout) {
            msgArrived = true;
        }
        // Unreceive in any case, to not let any ghost receiver.
        auto unreceived = unreceive(msgToSend.reply_to());
        if (!unreceived) {
            logWarn("Issue on unreceive");
        }

        // TODO: To rework with common promise
        // Unreceive filter
        auto unreceivedFilter = m_clientPtr->unreceiveFilter(proton::to_string(msgToSend.correlation_id()));
        if (unreceivedFilter != DeliveryState::Accepted) {
            logWarn("Issue on unreceive filter");
        }

        if (!msgArrived) {
            logError("No message arrived within {} seconds!", timeoutInSeconds);
            return fty::unexpected(DeliveryState::Timeout);
        }

        return futureSynRequest.get();
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

} // namespace fty::messagebus2::amqp

