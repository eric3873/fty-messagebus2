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

#include "AmqpClient.h"
#include <fty_log.h>
#include <proton/connection_options.hpp>
#include <proton/reconnect_options.hpp>
#include <proton/tracker.hpp>
#include <proton/work_queue.hpp>
#include <proton/session_options.hpp>
#include <proton/sender_options.hpp>
#include <proton/target.hpp>

namespace {
    proton::reconnect_options reconnectOpts()
    {
        proton::reconnect_options reconnectOption;
        reconnectOption.delay(proton::duration::SECOND);
        reconnectOption.delay_multiplier(5);
        reconnectOption.max_delay(proton::duration::FOREVER);
        reconnectOption.max_attempts(0);
        return reconnectOption;
    }

    proton::connection_options connectOpts()
    {
        proton::connection_options opts;
        opts.idle_timeout(proton::duration(2000));
        //opts.idle_timeout(proton::duration(proton::duration::FOREVER));
        return opts;
    }
} // namespace

namespace fty::messagebus2::amqp {
using namespace fty::messagebus2;
using MessageListener = fty::messagebus2::MessageListener;

static auto constexpr TIMEOUT_MS = 5000;

AmqpClient::AmqpClient(const Endpoint& url)
    : m_url(url), m_pool(std::make_shared<fty::messagebus2::utils::PoolWorker>(10))
{
}

AmqpClient::~AmqpClient()
{
    close();
}

void AmqpClient::on_container_start(proton::container& container)
{
    try {
        container.connect(m_url, connectOpts().reconnect(reconnectOpts()));
        container.auto_stop(false);
        logDebug("Open container {} on url {}", container.id(), m_url);
    } catch (const std::exception& e) {
        logError("Exception {}", e.what());
        m_connectPromise.setValue(ComState::ConnectFailed);
    }
}

void AmqpClient::on_container_stop(proton::container& container)
{
    logDebug("Close container {} ...", container.id());
    m_containerStop.notify_all();
}

void AmqpClient::on_connection_open(proton::connection& connection)
{
    m_connection = connection;
    if (connection.reconnected()) {
        logDebug("Reconnected on url: {}", m_url);
        resetPromises();
    } else {
        logDebug("Connected on url: {}", m_url);
    }
    m_connectPromise.setValue(ComState::Connected);
}

void AmqpClient::on_connection_close(proton::connection&)
{
    logDebug("Close connection ...");
    m_deconnectPromise.setValue();
}

void AmqpClient::on_connection_error(proton::connection& connection)
{
    logError("Error connection {}", connection.error().what());

    // On connection error, the connection is closed and the communication is definitively
    // closed. In this particular case, the library never retry to reconnect the communication with
    // reconnection option parameters. The idea is to try to re-open directly the connection.
    // Note: The container must have the option auto_stop deactivated to not stop completely the container.
    // Without that, the communication restoration will be more difficult.

    m_connection = connection;
    connection.work_queue().add([=]() { m_connection.open(); });
}

void AmqpClient::on_session_open(proton::session& session)
{
    logDebug("Open session ...");
    m_session = session;
    m_promiseSession.setValue();
}

void AmqpClient::on_session_close(proton::session&)
{
    logDebug("Close session ...");
    m_promiseSessionClose.setValue();
}

void AmqpClient::on_sender_open(proton::sender& sender)
{
    logDebug("Open sender {} ...", sender.name());
    try {
        sender.send(m_message);
        sender.close();
        m_promiseSender.setValue();
        logDebug("Message sent");
    }
    catch (const std::exception& e) {
        logWarn("on_sender_open error: {}", e.what());
    }
}

void AmqpClient::on_sender_close(proton::sender& sender)
{
    logDebug("Close sender {} ...", sender.name());
    m_promiseSender.setValue();
}

void AmqpClient::on_receiver_open(proton::receiver& receiver)
{
    logDebug("Waiting any message on target address: {}", receiver.source().address());
    m_promiseReceiver.setValue();
}

void AmqpClient::on_receiver_close(proton::receiver& receiver)
{
    logDebug("Close receiver {}...", receiver.name());
    m_promiseReceiver.setValue();
}

void AmqpClient::on_error(const proton::error_condition& error)
{
    logError("Protocol error: {}", error.what());
}

void AmqpClient::on_transport_error(proton::transport& transport)
{
    logError("Transport error: {}", transport.error().what());
    // Reset connect promise in case of send or receive arrived before connection open
    m_connectPromise.reset();
    m_communicationState = ComState::Lost;
}

void AmqpClient::on_transport_open(proton::transport&)
{
    logDebug("Open transport ...");
}

void AmqpClient::on_transport_close(proton::transport&)
{
    logDebug("Transport close ...");
}

void AmqpClient::on_message(proton::delivery&, proton::message& msg)
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Message arrived: {}", proton::to_string(msg));
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection.active()) {
        std::string correlationId = msg.correlation_id().empty() || !msg.reply_to().empty() ? "" : proton::to_string(msg.correlation_id());
        std::string key = setAddressFilterKey(msg.address(), correlationId);
        if (auto it {m_subscriptions.find(key)}; it != m_subscriptions.end()) {
            // Message listener called by qpid-proton library

            // NOTE: Need to execute the callback in another pool of thread. The callback
            // treatment is executing in the same worker as sender/receiver and hang the response.
            // TODO: Don't work with another proton worker
            //m_workQueue.add(proton::make_work(it->second, amqpMsg));
            m_pool->offload(std::bind(it->second, amqpMsg));
            return;
        }
        else {
            logWarn("No message listener checked in for: {}", key);
        }
    } else {
        // Connection not set
        logError("Nothing to do, connection object not active");
    }
}

// Test if connected. If not, wait for the connection to be established (with timeout)
bool AmqpClient::isConnected() {
    // test if connected
    return (m_connection && m_connection.active() && connected() == ComState::Connected);
}

// If not connected, wait for the connection to be established (with timeout)
ComState AmqpClient::connected()
{
    // Wait communication was restored
    if (m_communicationState != ComState::Connected) {
        if (m_connectPromise.waitFor(TIMEOUT_MS)) {
            if (auto value = m_connectPromise.getValue(); value) {
                m_communicationState = *value;
            }
        } else {
            m_communicationState = ComState::ConnectFailed;
        }
    }
    return m_communicationState;
}

// Send a message
DeliveryState AmqpClient::send(const proton::message& msg)
{
    auto deliveryState = DeliveryState::Rejected;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);

        logDebug("Sending message to {} ...", msg.to());
        m_message.clear();
        m_message = msg;

        // Note for memory leak issue: The senders and the receivers which are closed are not removed from current
        // session list and leak memory. The solution is to open a new session for each new sender and close the
        // session when the message is sent to close properly the sender.

        m_promiseSession.reset();
        // Open a new temporary session for the transaction
        m_connection.work_queue().add([=]() {
            m_connection.open_session();
        });
        // Wait the session creation
        if (!m_promiseSession.waitFor(TIMEOUT_MS)) {
            logError("Send error, unable to open a new session for {}", msg.to());
            return deliveryState;
        }

        // Create the sender in the session
        m_promiseSender.reset();
        m_connection.work_queue().add([=]() {
            m_session.open_sender(msg.to(), proton::sender_options().name(msg.to()));
        });
        // Wait to know if the message has been sent or not
        if (m_promiseSender.waitFor(TIMEOUT_MS)) {
            deliveryState = DeliveryState::Accepted;
        }

        // Close the temporary session
        m_connection.work_queue().add([&]() {
            m_session.close();
        });
    }
    return deliveryState;
}

// Subscribe a receiver to an address with optional filter
DeliveryState AmqpClient::receive(const Address& address, MessageListener messageListener, const std::string& filter)
{
    auto deliveryState = DeliveryState::Rejected;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);
        logDebug("Set receiver to wait message(s) from {} ...", address);
        m_promiseReceiver.reset();

        auto key = setAddressFilterKey(address, filter);
        if (!setSubscriptions(key, messageListener)) {
            return deliveryState;
        }

        // Note for memory leak issue: The senders and the receivers which are closed are not removed from current
        // session list and leak memory. The solution is to open a new session for each new receiver and close the
        // session when the message is received to close properly the receiver.

        // Open a new temporary session for the transaction
        m_promiseSession.reset();
        m_connection.work_queue().add([&]() {
            m_connection.open_session();
        });
        // Wait the session creation
        if (!m_promiseSession.waitFor(TIMEOUT_MS)) {
            logError("Send error, unable to open a new session for {}", key);
            return deliveryState;
        }

        // Create receiver in the session
        m_promiseReceiver.reset();
        m_connection.work_queue().add([&]() {
            m_session.open_receiver(address, proton::receiver_options().name(key).auto_accept(true));
        });
        // Wait to know if the receiver has been created
        if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
            deliveryState = DeliveryState::Accepted;
        }
        else {
            logError("Error on receive for {}, timeout reached", address);
        }
    }
    return deliveryState;
}

// Unreceive an address with optional filter
DeliveryState AmqpClient::unreceive(const Address& address, const std::string& filter)
{
    auto deliveryState = DeliveryState::Unavailable;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);

        auto key = setAddressFilterKey(address, filter);
        // Remove key in subscriptions list
        if (!unsetSubscriptions(key)) {
            logError("Error on unreceive for {}: impossible to remove key in subscriptions list", key);
            return deliveryState;
        }

        // find receiver with this address and close it with the session
        bool isFound = false;
        auto receivers = m_connection.receivers();
        logDebug("unreceive: try to close receiver {}", address);
        for (auto receiver : receivers) {
            if (receiver.name() == address && !receiver.closed() && receiver.active()) {
                isFound = true;
                auto session = receiver.session();
                m_promiseReceiver.reset();
                m_connection.work_queue().add([&]() {
                    receiver.close();
                });
                if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
                    logDebug("Receiver closed for {}", address);
                    deliveryState = DeliveryState::Accepted;
                } else {
                    logError("Error on unreceive for {}, timeout reached", address);
                }
                session.close();
                break;
            }
        }
        if (!isFound)  {
            logWarn("Unable to unreceive address {}: not present", address);
        }
    }
    return deliveryState;
}

// Close connection
void AmqpClient::close()
{
    std::lock_guard<std::mutex> lock(m_lock);

    if (m_connection && m_connection.active()) {

        // First close sessions still opened
        auto sessions = m_connection.sessions();
        for (auto session : sessions) {
            if (!session.closed()) {
                m_promiseSessionClose.reset();
                m_connection.work_queue().add([&]() {
                    logDebug("Close session");
                    session.close();
                });
                // Wait the session creation
                if (!m_promiseSessionClose.waitFor(TIMEOUT_MS)) {
                    logError("Unable to close session: timeout");
                }
            }
        }

        // Wait end of connection (with timeout)
        m_deconnectPromise.reset();
        m_connection.work_queue().add([&]() { m_connection.close(); });
        if (!m_deconnectPromise.waitFor(TIMEOUT_MS)) {
            logError("AmqpClient::close De-connection timeout reached");
        }

        // Close the container
        logTrace("Start stop container");
        m_connection.container().stop();
        std::unique_lock<std::mutex> lockStop(m_lockStop);
        auto status = m_containerStop.wait_for(lockStop, std::chrono::seconds(5));
        if (status == std::cv_status::timeout) {
            logWarn("End stop container with timeout");
        }
        logTrace("End stop container");
    }
}

// Reset all promises
void AmqpClient::resetPromises()
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Reset all promises");
    m_connectPromise.reset();
    m_deconnectPromise.reset();
    m_promiseSession.reset();
    m_promiseSessionClose.reset();
    m_promiseSender.reset();
    m_promiseReceiver.reset();
}

// Set subscription designed by a key and a message listener
bool AmqpClient::setSubscriptions(const std::string& key, MessageListener messageListener)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (!key.empty() && messageListener) {
        if (auto it {m_subscriptions.find(key)}; it == m_subscriptions.end()) {
            auto ret = m_subscriptions.emplace(key, messageListener);
            if (!ret.second) {
                logWarn("Subscription not emplaced: {}", key);
            }
            else {
                logDebug("Subscription added: {}", key);
                return true;
            }
        } else {
            logWarn("Subscription skipped, key yet present: {}", key);
        }
    } else {
        logWarn("Subscription skipped, call back information or key not filled!");
    }
    return false;
}

// Unset subscription designed by the input key
bool AmqpClient::unsetSubscriptions(const std::string& key)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (!key.empty()) {
        if (auto it {m_subscriptions.find(key)}; it != m_subscriptions.end()) {
            m_subscriptions.erase(it);
            logDebug("Subscriptions remove: {}", key);
            return true;
        } else {
            logWarn("unsetSubscriptions skipped, key not found: {}", key);
        }
    } else {
        logWarn("unsetSubscriptions skipped, key empty");
    }
    return false;
}

} // namespace fty::messagebus2::amqp
