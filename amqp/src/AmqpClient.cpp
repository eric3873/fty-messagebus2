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
    : m_url(url)
{
    m_pool = new fty::ThreadPool(10, 100);
}

AmqpClient::~AmqpClient()
{
    close();

    if (m_pool) {
        delete m_pool;
    }
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
    {
        std::unique_lock<std::mutex> lock(m_lockContainerStop);
        m_containerStopOk = true;
    }
    m_cvContainerStop.notify_one();
}

void AmqpClient::on_connection_open(proton::connection& connection)
{
    m_connection = connection;
    if (connection.reconnected()) {
        logDebug("Reconnected on url: {}", m_url);
        resetPromises();
    }
    else {
        logDebug("Connected on url: {}", m_url);
    }
    m_connectPromise.setValue(ComState::Connected);
}

void AmqpClient::on_connection_close(proton::connection&)
{
    logDebug("Close connection ...");
    m_cvConnectionClose.notify_one();
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
    connection.work_queue().add([&]() { m_connection.open(); });
}

void AmqpClient::on_session_open(proton::session&)
{
    logDebug("Open session ...");
}

void AmqpClient::on_session_close(proton::session&)
{
    logDebug("Close session ...");
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
    m_receiver = receiver;
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
    if (m_communicationState != ComState::Unknown) {
        m_connectPromise.reset();
        m_communicationState = ComState::Lost;
    }
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
            // TODO: Don't use custom PoolWorker which crash with multithread test
            m_pool->pushWorker(std::bind(it->second, amqpMsg));
            return;
        }
        else {
            logWarn("No message listener checked in for: {}", key);
        }
    }
    else {
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
        }
        else {
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

        // For request/reply with temporary queue, need to retreive the name of this temporary queue which has
        // been created. The name of this queue has been saved in subscriptions list (receive called with empty
        // address and a filter not empty). Use the unique filter (correlation_id) to retrieve the queue name.
        if (m_message.reply_to().empty()) {
            auto address = getAddressSubscriptions(proton::to_string(m_message.correlation_id()));
            // Set reply_to only if request/reply (address found in subscriptions list)
            if (!address.empty()) {
                logDebug("Set reply_to with address={}", address);
                m_message.reply_to(address);
            }
        }

        // Create the sender
        m_promiseSender.reset();
        m_connection.work_queue().add([&]() {
            m_connection.open_sender(m_message.to(), proton::sender_options().name(m_message.to()));
        });
        // Wait to know if the message has been sent or not
        if (m_promiseSender.waitFor(TIMEOUT_MS)) {
            deliveryState = DeliveryState::Accepted;
        }
    }
    return deliveryState;
}

// Subscribe a callback to an address with an optional filter.
// To create a temporary queue for the receiver (request/reply), use an empty address in input.
// The filter must be unique and is mandatory for temporary queue and for filter a message with the same address.
DeliveryState AmqpClient::receive(const Address& address, MessageListener messageListener, const std::string& filter)
{
    auto deliveryState = DeliveryState::Rejected;

    // Test input parameters
    if (address.empty() && filter.empty()) {
        logError("Receive bad parameters");
        return deliveryState;
    }

    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);
        logDebug("Set receiver to wait message(s) from {} ...", address);
        m_promiseReceiver.reset();

        proton::source_options opts;

        // TBD: Reuse receiver if defined for address
        std::string receiverName = address;
        // If defined (not empty), set filter on receiver
        if (!filter.empty()) {
            // For temporary queue, the receiver name must be empty and the dynamic option must be set
            // for the receiver. A new receiver will be created with a unique name by the broker.
            // The queue will be removed when the receiver will be closed.
            // In this case, no need to filter with correlation id as the queue name is unique.
            if (address.empty()) {
                opts.dynamic(true);
                // Receiver name is ignored
                receiverName = "";
            }
            else {
                // Receiver with filtering, so reply, the filtering for this implementation is only on correlationId
                std::ostringstream correlIdFilter;
                correlIdFilter << "JMSCorrelationID";
                correlIdFilter << "='";
                correlIdFilter << filter;
                correlIdFilter << "'";
                logTrace("CorrelId filter: {}", correlIdFilter.str());

                proton::source::filter_map map;
                proton::symbol filterKey("selector");
                proton::value filterValue;
                // The value is a specific AMQP "described type": binary string with symbolic descriptor
                // (See APACHE.ORG:SELECTOR on http://www.amqp.org/specification/1.0/filters)
                proton::codec::encoder enc(filterValue);
                enc << proton::codec::start::described()
                    << proton::symbol("apache.org:selector-filter:string")
                    << correlIdFilter.str()
                    << proton::codec::finish();
                // In our case, the map has this one element
                map.put(filterKey, filterValue);
                opts.filters(map);

                receiverName = filter;
                // TBD: The source is discarded immediately when the link is closed (LINK_CLOSE by default)
                opts.timeout(proton::duration::IMMEDIATE);
            }
        }

        // Create receiver
        m_promiseReceiver.reset();
        m_connection.work_queue().add([&]() {
            m_connection.open_receiver(address, proton::receiver_options().name(receiverName).source(opts).auto_accept(true));
        });
        // Wait to know if the receiver has been created
        if (!m_promiseReceiver.waitFor(TIMEOUT_MS)) {
            logError("Error on receive for {}, timeout reached", address);
            return deliveryState;
        }

        // For tempory queue (address is empty), get the unique address created by the broker
        Address address_ = address;
        if (address_.empty()) {
            address_ = m_receiver.source().address();
            logDebug("Get new temporary receiver address {} for {}", address_, filter);
        }

        // Then add address in subscription list
        auto key = setAddressFilterKey(address_, filter);
        if (!setSubscriptions(key, messageListener)) {
            return deliveryState;
        }

        deliveryState = DeliveryState::Accepted;
    }
    return deliveryState;
}

// Unreceive a callback with the address and the filter specified.
// The filter is facultative. For temporary queue for the receiver, use an empty address
// and the filter use during creation.
DeliveryState AmqpClient::unreceive(const Address& address, const std::string& filter)
{
    auto deliveryState = DeliveryState::Unavailable;

    // Test input parameters
    if (address.empty() && filter.empty()) {
        logError("Unreceive bad parameters");
        return deliveryState;
    }

    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);

        // Retreive the name of the receiver to close.
        // By default, the name of the receiver is the address or the filter if not empty.
        // Note with temporary queue use for request/reply: the address is empty and need to be retreived
        // in subscriptions list (receive called with empty address and a filter not empty).
        // In this case, use the filter (correlation_id) to retrieve the address.
        Address address_ = address;
        std::string receiverNameToSearch = address_;
        if (address_.empty()) {
            address_ = getAddressSubscriptions(filter);
            receiverNameToSearch = address_;
        }
        else if (!filter.empty()) {
            receiverNameToSearch = filter;
        }

        // Find receiver with this address and close it
        bool isFound = false;
        logDebug("unreceive: try to close receiver {}", address_);
        auto receivers = m_connection.receivers();
        for (auto receiver : receivers) {
            logTrace("test receiver.name()={} address={} closed={} active={}",
                receiver.name(), receiver.source().address(), receiver.closed(), receiver.active());

            if ((receiver.name() == receiverNameToSearch) ||
                (receiver.name().empty() && receiver.source().address() == address_)) {
                isFound = true;
                // Close receiver if needed
                if (!receiver.closed() && receiver.active()) {
                    m_promiseReceiver.reset();
                    m_connection.work_queue().add([&]() {
                        receiver.close();
                    });
                    if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
                        logDebug("Receiver closed for {}", address_);
                        deliveryState = DeliveryState::Accepted;
                    }
                    else {
                        logError("Error on unreceive for {}, timeout reached", address_);
                    }
                }
                else {
                    logWarn("Unable to close receiver for address {}: still closed", address_);
                    deliveryState = DeliveryState::Accepted;
                }
                break;
            }
        }
        if (!isFound)  {
            logWarn("Unable to unreceive address {}: not present", address_);
        }

        // Then remove key from subscriptions list
        auto key = setAddressFilterKey(address_, filter);
        if (!unsetSubscriptions(key)) {
            logError("Error on unreceive for {}: impossible to remove key in subscriptions list", key);
            deliveryState = DeliveryState::Unavailable;
        }
    }
    return deliveryState;
}

// Close connection
void AmqpClient::close()
{
    std::lock_guard<std::mutex> lock(m_lock);

    if (m_connection && m_connection.active()) {

        // First close the connection
        m_connection.work_queue().add([&]() {
            m_connection.close();
        });
        std::unique_lock<std::mutex> lockConnectionClose(m_lockConnectionClose);
        auto statusConnection = m_cvConnectionClose.wait_for(lockConnectionClose, std::chrono::seconds(5));
        if (statusConnection == std::cv_status::timeout) {
            logWarn("End stop connection with timeout");
        }

        // Thread to wait end of container
        std::thread thrd([this]() {
            std::unique_lock<std::mutex> lockContainerStop(m_lockContainerStop);
            auto statusContainer = m_cvContainerStop.wait_for(lockContainerStop, std::chrono::seconds(5), [this]{
                return m_containerStopOk;
            });
            if (!statusContainer) {
                logWarn("End stop container with timeout");
            }
        });

        // Then close the container
        m_connection.container().stop();
        thrd.join();
    }
}

// Reset all promises
void AmqpClient::resetPromises()
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Reset all promises");
    m_connectPromise.reset();
    m_promiseSender.reset();
    m_promiseReceiver.reset();
}

// Get address from filter (use for reply with temporary queue)
std::string AmqpClient::getAddressSubscriptions(const std::string& filter) {
    for (auto it = m_subscriptions.begin(); it != m_subscriptions.end(); ++it) {
        auto addressFilter = getAddressFilterKey(it->first);
        if (addressFilter.second == filter) {
            return addressFilter.first;
        }
    }
    return std::string("");
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
        }
        else {
            logWarn("Subscription skipped, key yet present: {}", key);
        }
    }
    else {
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
        }
        else {
            logWarn("unsetSubscriptions skipped, key not found: {}", key);
        }
    }
    else {
        logWarn("unsetSubscriptions skipped, key empty");
    }
    return false;
}

} // namespace fty::messagebus2::amqp
