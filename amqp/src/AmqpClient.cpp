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
    } catch (const std::exception& e) {
        logError("Exception {}", e.what());
        m_connectPromise.setValue(ComState::ConnectFailed);
    }
}

void AmqpClient::on_container_stop(proton::container&)
{
    logInfo("Close container ...");
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

void AmqpClient::on_sender_open(proton::sender& sender)
{
    logDebug("Open sender ...");
    try {
        sender.send(m_message);
        // Due to a limitation of the library, the sender in cache cannot be closed and reopened as needed (TBD).
        //sender.close();
        m_promiseSender.setValue();
        logDebug("Message sent");
    }
    catch (const std::exception& e) {
        logWarn("on_sender_open error: {}", e.what());
    }
}

void AmqpClient::on_sender_close(proton::sender&)
{
    logDebug("Close sender ...");
}

void AmqpClient::on_receiver_open(proton::receiver& receiver)
{
    logDebug("Waiting any message on target address: {}", receiver.source().address());
    m_promiseReceiver.setValue();
}

void AmqpClient::on_receiver_close(proton::receiver&)
{
    logDebug("Close receiver ...");
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

void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Message arrived: {}", proton::to_string(msg));
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection.active()) {
        std::string correlationId = msg.correlation_id().empty() || !msg.reply_to().empty() ? "" : proton::to_string(msg.correlation_id());
        std::string key = setAddressFilterKey(msg.address(), correlationId);
        if (auto it {m_subscriptions.find(key)}; it != m_subscriptions.end()) {
            delivery.accept();
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
    logDebug("Message rejected: {}", proton::to_string(msg));
    delivery.reject();
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
        m_promiseSender.reset();
        logDebug("Sending message to {} ...", msg.to());
        m_message.clear();
        m_message = msg;

        // Try to find a sender which has been already opened for this message address (internal cache)
        bool isFound = false;
        auto senders = m_connection.default_session().senders();
        logDebug("Try to find a sender open available for {}", msg.to());
        for (auto sender : senders) {
            logTrace("Test sender name={} closed={} active={}", sender.name(), sender.closed(), sender.active());
            if (sender.name() == msg.to() && !sender.closed() && sender.active()) {
                logDebug("Find sender {}", sender.name());
                try {
                    // Due to a limitation of the library, the sender in cache cannot be closed and reopened as needed (TBD).
                    //sender.open();
                    sender.send(msg);
                    //sender.close();
                    logDebug("Message sent");
                    deliveryState = DeliveryState::Accepted;
                    isFound = true;
                }
                catch (const std::exception& e) {
                    logWarn("send error: {}", e.what());
                }
            }
        }
        if (!isFound)  {
            logDebug("Unable to find a sender for {}, create a new one", msg.to());
            m_connection.work_queue().add([=]() {
                m_connection.default_session().open_sender(msg.to(), proton::sender_options().name(msg.to()));
            });
            // Wait to know if the message has been sent or not
            if (m_promiseSender.waitFor(TIMEOUT_MS)) {
                deliveryState = DeliveryState::Accepted;
            }
        }
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

        // Try to find a receiver which has been already opened for this address (internal cache)
        bool isFound   = false;
        auto receivers = m_connection.default_session().receivers();
        logDebug("Try to find a receiver open available for {}", address);
        for (auto receiver : receivers) {
            logTrace("Test receiver name={} closed={} active={}", receiver.name(), receiver.closed(), receiver.active());
            if (receiver.name() == address && !receiver.closed() && receiver.active()) {
                logDebug("Find receiver {}", receiver.name());
                deliveryState = DeliveryState::Accepted;
                isFound = true;
            }
        }
        if (!isFound)  {
            logDebug("Unable to find a receiver for {}, create a new one", address);
            m_connection.work_queue().add([=]() {
                m_connection.default_session().open_receiver(address, proton::receiver_options().name(address).auto_accept(false));
            });

            if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
                deliveryState = DeliveryState::Accepted;
            }
            else {
                logError("Error on receive for {}, timeout reached", address);
            }
        }
    }
    return deliveryState;
}

// Unreceive an address with optional filter
// CAUTION: If forceClose activated (deactivated by default), close definitely the receiver which cause memory leak
DeliveryState AmqpClient::unreceive(const Address& address, const std::string& filter, bool forceClose/*=false*/)
{
    auto deliveryState = DeliveryState::Unavailable;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);

        auto key = setAddressFilterKey(address, filter);
        // Remove key in subscriptions list
        if (unsetSubscriptions(key) && !forceClose) {
            deliveryState = DeliveryState::Accepted;
        }

        // If forse option is actived, close definitely the receiver
        // Caution: In this case, due to a limitation of the library, the internal receiver
        // is not cleared and memory leak !!!
        if (forceClose) {
            // If address is no longer used in all receivers,
            // then find receiver with this address and close it
            if (!isAddressInSubscriptions(address)) {
                bool isFound = false;
                auto receivers = m_connection.default_session().receivers();
                logDebug("unreceive: try to close {}", address);
                for (auto receiver : receivers) {
                    if (receiver.name() == address && !receiver.closed() && receiver.active()) {
                        isFound = true;
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
                    }
                }
                if (!isFound)  {
                    logWarn("Unable to unreceive address {}: not present", address);
                }
            }
        }
    }
    return deliveryState;
}

// Close connection
void AmqpClient::close()
{
    std::lock_guard<std::mutex> lock(m_lock);

    if (m_connection && m_connection.active()) {

        // Make sure that all senders and receivers are closed.
        // Note for memory leak issue: The senders and the receivers which are closed are not removed from current session list and
        // leak memory. Instead of create a sender or a receiver for each message to send, create one in cache for each different queue.
        // Workaround due to a limitation of the library, the sender or the receiver in cache cannot be closed and reopened as needed (TBD).
        // That's why we close all the senders and all the receivers in the current session when the connection is closed.

        // First for senders
        auto senders = m_connection.default_session().senders();
        for (auto sender : senders) {
            if (!sender.closed()) {
                sender.close();
            }
        }

        // Do the same with receivers
        auto receivers = m_connection.default_session().receivers();
        for (auto receiver : receivers) {
            if (!receiver.closed()) {
                receiver.close();
            }
        }

        // Then wait end of connection (with timeout)
        m_deconnectPromise.reset();
        m_connection.work_queue().add([=]() { m_connection.close(); });
        if (!m_deconnectPromise.waitFor(TIMEOUT_MS)) {
            logError("AmqpClient::close De-connection timeout reached");
        }
    }
}

// Reset all promises
void AmqpClient::resetPromises()
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Reset all promises");
    m_connectPromise.reset();
    m_deconnectPromise.reset();
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

// Test if the input address is present in the subscription list
bool AmqpClient::isAddressInSubscriptions(const Address& address)
{
    if (!address.empty()) {
        // Search if address is present in a subscription
        for (const auto& subscription : m_subscriptions) {
            auto key = subscription.first;
            auto addressFilter = getAddressFilterKey(key);
            if (addressFilter.first == address) {
                logDebug("isAddressInSubscriptions find: {}", address);
                return true;
            }
        }
    } else {
        logWarn("isAddressInSubscriptions skipped, address empty");
    }
    return false;
}

} // namespace fty::messagebus2::amqp
