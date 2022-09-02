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

//void AmqpClient::on_connection_wake(proton::connection&)
//{
//    logDebug("Wake connection ...");
//}

void AmqpClient::on_sender_open(proton::sender&)
{
    logDebug("Open sender ...");
}

void AmqpClient::on_sendable(proton::sender& sender)
{
    logDebug("Sending message ...");
    // TODO: DON'T WORK !!!!
    //sender.work_queue().add([&]() {
        try {
            sender.send(m_message);
            sender.close();
            m_promiseSender.setValue();
            logDebug("Message sent");
        }
        catch (const std::exception& e) {
           logWarn("on_sendable error: {}", e.what());
        }
    //});
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

void AmqpClient::resetPromises()
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Reset all promises");
    m_connectPromise.reset();
    m_deconnectPromise.reset();
    m_promiseSender.reset();
    m_promiseReceiver.reset();
}

bool AmqpClient::isConnected() {
    // test if connected
    return (m_connection && m_connection.active() && connected() == ComState::Connected);
}

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

DeliveryState AmqpClient::send(const proton::message& msg)
{
    auto deliveryState = DeliveryState::Rejected;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);
        m_promiseSender.reset();
        logDebug("Sending message to {} ...", msg.to());
        m_message.clear();
        m_message = msg;
        m_connection.work_queue().add([=]() {
            m_connection.default_session().open_sender(msg.to());
        });
        // Wait to know if the message has been sent or not
        if (m_promiseSender.waitFor(TIMEOUT_MS)) {
            deliveryState = DeliveryState::Accepted;
        }
    }
    return deliveryState;
}

DeliveryState AmqpClient::receive(const Address& address, MessageListener messageListener, const std::string& filter)
{
    auto deliveryState = DeliveryState::Rejected;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);
        logDebug("Set receiver to wait message(s) from {} ...", address);
        m_promiseReceiver.reset();

        auto name = setAddressFilter(address, filter);
        setSubscriptions(name, messageListener);

        m_connection.work_queue().add([=]() {
            m_connection.default_session().open_receiver(address, proton::receiver_options().name(name).auto_accept(false));
        });

        if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
            deliveryState = DeliveryState::Accepted;
        }
        else {
            logError("Error on receive for {}, timeout reached", address);
        }
    }
    return deliveryState;
}

void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Message arrived: {}", proton::to_string(msg));
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection.active()) {
        std::string correlationId = msg.correlation_id().empty() || !msg.reply_to().empty() ? "" : proton::to_string(msg.correlation_id());
        std::string key = setAddressFilter(msg.address(), correlationId);
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

void AmqpClient::setSubscriptions(const Address& address, MessageListener messageListener)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (!address.empty() && messageListener) {
        if (auto it {m_subscriptions.find(address)}; it == m_subscriptions.end()) {
            auto ret = m_subscriptions.emplace(address, messageListener);
            logDebug("Subscriptions added: {}", address);
            if (!ret.second) {
                logWarn("Subscriptions not emplaced: {}", address);
            }
        } else {
            logWarn("Subscriptions skipped, address yet present: {}", address);
        }
    } else {
        logWarn("Subscriptions skipped, call back information not filled!");
    }
}

void AmqpClient::unsetSubscriptions(const Address& address)
{
    std::lock_guard<std::mutex> lock(m_lock);
    if (!address.empty()) {
        if (auto it {m_subscriptions.find(address)}; it != m_subscriptions.end()) {
            m_subscriptions.erase(it);
            logDebug("Subscriptions remove: {}", address);
        } else {
            logWarn("unsetSubscriptions skipped, address not found: {}", address);
        }
    } else {
        logWarn("unsetSubscriptions skipped, address empty");
    }
}

DeliveryState AmqpClient::unreceive(const Address& address, const std::string& filter)
{
    auto deliveryState = DeliveryState::Unavailable;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);

        auto receiverName = setAddressFilter(address, filter);
        // Remove first address in subscriptions list
        unsetSubscriptions(receiverName);

        // Then find recever with input address and close it
        bool isFound = false;
        auto receivers = m_connection.default_session().receivers();
        logDebug("unreceive: try to close {}", receiverName);
        for (auto receiver : receivers) {
            if (receiver.name() == receiverName && !receiver.closed() && receiver.active()) {
                isFound = true;
                m_promiseReceiver.reset();
                m_connection.work_queue().add([&]() {
                    receiver.close();
                });
                if (m_promiseReceiver.waitFor(TIMEOUT_MS)) {
                    logDebug("Receiver closed for {}", receiverName);
                    deliveryState = DeliveryState::Accepted;
                } else {
                    logError("Error on unreceive for {}, timeout reached", receiverName);
                }
            }
        }
        if (!isFound)  {
            logWarn("Unable to unreceive address {}: not present", receiverName);
        }
    }
    return deliveryState;
}

void AmqpClient::close()
{
    std::lock_guard<std::mutex> lock(m_lock);
    m_deconnectPromise.reset();
    if (m_connection && m_connection.active()) {
        m_connection.work_queue().add([=]() { m_connection.close(); });

        if (!m_deconnectPromise.waitFor(TIMEOUT_MS)) {
            logError("AmqpClient::close De-connection timeout reached");
        }
    }
}

} // namespace fty::messagebus2::amqp
