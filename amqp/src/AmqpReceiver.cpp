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

#include <qpid/messaging/Receiver.h>
#include <qpid/messaging/Session.h>

#include <fty_log.h>

namespace fty::messagebus2::amqp {

AmqpReceiver::AmqpReceiver(amqp::AmqpClient *client, const std::string& address) :
    m_client(client),
    m_address(address),
    m_closed(false)
{
}

AmqpReceiver::~AmqpReceiver()
{
    // If not closed, wait end of messaging thread
    if (!m_closed) {
        waitClose();
    }
}

bool AmqpReceiver::init(const std::string& filter, const MessageListener& messageListener)
{
    if (!m_client) {
        logError("Receiver init error: client not defined");
        return false;
    }
    auto connection = m_client->getConnection();
    if (!connection) {
        logError("Receiver init error: no connection");
        return false;
    }
    auto qpidReceiver = connection->getSession(DEFAULT_SESSION).createReceiver(m_address);
    m_name = qpidReceiver.getName();
    // Create first subscription with input parameters
    if (!setSubscription(filter, messageListener)) {
        logError("Receiver init error: bad subscription");
        return false;
    }
    // Create thread for receive message
    std::thread thread(&AmqpReceiver::manageMessage, this);
    thread.detach();
    return true;
}

bool AmqpReceiver::waitClose()
{
    m_closed = true;
    // Wait until messaging thread terminate
    if (!m_promiseClose.waitFor(TIMEOUT_MS)) {
        return false;
    }
    return true;
}

ulong AmqpReceiver::getSubscriptionsNumber()
{
    return m_subscriptions.size();
}

MessageListener AmqpReceiver::getSubscription(const std::string& filter)
{
    std::lock_guard<std::mutex> lock(m_lock);
    auto it = m_subscriptions.find(filter);
    if (it != m_subscriptions.end()) {
        return (*it).second;
    }
    return nullptr;
}

bool AmqpReceiver::setSubscription(const std::string& filter, MessageListener messageListener)
{
    if (messageListener) {
        std::lock_guard<std::mutex> lock(m_lock);
        if (auto it {m_subscriptions.find(filter)}; it == m_subscriptions.end()) {
            logDebug("Subscriptions added: {} / {}", m_address, filter);
            m_subscriptions.emplace(filter, messageListener);
            return true;

        } else {
            logWarn("Subscriptions skipped, filter yet present: {} / {}", m_address, filter);
        }
    } else {
        logWarn("Subscriptions skipped, call back information not filled!");
    }
    return false;
}

bool AmqpReceiver::unsetSubscription(const std::string& filter)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (auto it {m_subscriptions.find(filter)}; it != m_subscriptions.end()) {
        logDebug("Subscriptions remove: {} / {}", m_address, filter);
        m_subscriptions.erase(it);
        return true;

    } else {
        logWarn("unsetSubscriptions skipped, filter not found: {} / {}", m_address, filter);
    }
    return false;
}

void AmqpReceiver::manageMessage()
{
    // Construct friendly description
    std::string desc = m_client->getName() + "(" + m_name + ")";

    // While connection not closed or unreceive not called
    while (!m_client->isClosed() && !m_closed) {
        try {
            auto connection = m_client->getConnection();

            // TODO
            // Try to reopen the connection if closed
            if (!connection->isOpen()) {
                logDebug("Reconnect detected for {}", desc);
                connection->reconnect();
            }

            qpid::messaging::Message message;
            auto receiver = connection->getSession(DEFAULT_SESSION).getReceiver(m_name);
            // Wait a new message arrived before timeout
            if (receiver.fetch(message, qpid::messaging::Duration::SECOND * 1)) {
                Message amqpMsg = getMessage(message);
                logDebug("Receive message on {}", desc);
                logTrace("{}", amqpMsg.toString());
                // Take into account correlation id for filter if not the reply sentence
                std::string correlationId = message.getReplyTo().str().empty() ? message.getCorrelationId() : "";

                // Test if the message filter match
                auto callback = getSubscription(correlationId);
                if (callback) {
                    logDebug("Acknowledge message with \"{}\" correlationId for {}", correlationId, desc);
                    receiver.getSession().acknowledge(message);

                    // Execute subscription callback
                    callback(amqpMsg);
                }
                else {
                    logDebug("Bad message receiver (correlationId: {}) for {}", correlationId, desc);
                }
            }
            logDebug("Ending manageMessage for {}", desc);

        }
        catch (const std::exception& ex) {
            logError("Exception in manageMessage {}: {}", desc, ex.what());
        }
    }

    // Indicate that the messaging thread is closing
    m_promiseClose.setValue();
}

} // namespace fty::messagebus2::amqp
