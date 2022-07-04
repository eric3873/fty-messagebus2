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
      reconnectOption.max_delay(proton::duration::MINUTE);
      reconnectOption.max_attempts(10);
      reconnectOption.delay_multiplier(5);
      return reconnectOption;
  }

  proton::connection_options connectOpts()
  {
      proton::connection_options opts;
      opts.idle_timeout(proton::duration(2000));
      return opts;
  }

} // namespace

namespace fty::messagebus2::amqp {
using namespace fty::messagebus2;
using MessageListener = fty::messagebus2::MessageListener;

static auto constexpr TIMEOUT = std::chrono::seconds(5);

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
    } catch (const std::exception& e) {
        logError("Exception {}", e.what());
        m_connectPromise.set_value(ComState::ConnectFailed);
    }
}

void AmqpClient::on_connection_open(proton::connection& connection)
{
    m_connection = connection;

    if (connection.reconnected()) {
        logDebug("Reconnected on url: {}", m_url);
        resetPromise();
    } else {
        logDebug("Connected on url: {}", m_url);
    }
    m_connectPromise.set_value(ComState::Connected);
}

void AmqpClient::on_connection_close(proton::connection&)
{
    logDebug("Close connection ...");
    m_deconnectPromise.set_value();
}

void AmqpClient::on_sender_open(proton::sender&)
{
    logDebug("Open sender ...");
}

void AmqpClient::on_sendable(proton::sender& sender)
{
    logDebug("Sending message ...");
    // TODO: DON'T WORK !!!!
    //sender.work_queue().add([&]() {
        sender.send(m_message);
        sender.close();
        m_promiseSender.set_value();
        logDebug("Message sent");
    //});
}

void AmqpClient::on_sender_close(proton::sender&)
{
    logDebug("Close sender ...");
}

void AmqpClient::on_receiver_open(proton::receiver& receiver)
{
    logDebug("Waiting any message on target address: {}", receiver.source().address());
    m_promiseReceiver.set_value();
}

void AmqpClient::on_receiver_close(proton::receiver&)
{
    logDebug("Close receiver ...");
    // TODO: Wait stop receiver dosn't work: exception !!!!
    //m_promiseReceiver.set_value();
}

void AmqpClient::on_error(const proton::error_condition& error)
{
    logError("Protocol error: {}", error.what());
}

void AmqpClient::on_transport_error(proton::transport& transport)
{
    logError("Transport error: {}", transport.error().what());
    // Reset connect promise in case of send or receive arrived before connection open
    m_connectPromise = std::promise<fty::messagebus2::ComState>();
    m_communicationState = ComState::Lost;
}

void AmqpClient::resetPromise()
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Reset all promise");
    m_connectPromise   = std::promise<fty::messagebus2::ComState>();
    m_deconnectPromise = std::promise<void>();
    m_promiseSender    = std::promise<void>();
    m_promiseReceiver  = std::promise<void>();
}

bool AmqpClient::isConnected() {
    // Wait communication was restored
    return (connected() == ComState::Connected);
}

ComState AmqpClient::connected()
{
   //logTrace("AmqpClient::connected m_communicationState={}", m_communicationState);
    if (m_communicationState != ComState::Connected) {
        auto connectFuture = m_connectPromise.get_future();
        if (connectFuture.wait_for(TIMEOUT) != std::future_status::timeout) {
            try {
                m_communicationState = connectFuture.get();
            } catch (const std::future_error& e) {
                logError("Caught future error {}", e.what());
            }
        } else {
            m_communicationState = ComState::ConnectFailed;
        }
    }
    //logTrace("AmqpClient::connected m_communicationState={}", m_communicationState);
    return m_communicationState;
}

DeliveryState AmqpClient::send(const proton::message& msg)
{
    auto deliveryState = DeliveryState::Rejected;
    if (isConnected()) {
        std::lock_guard<std::mutex> lock(m_lockMain);
        m_promiseSender = std::promise<void>();
        logDebug("Sending message to {} ...", msg.to());
        m_message.clear();
        m_message = msg;
        m_connection.work_queue().add([=]() {
            m_connection.open_sender(msg.to());
        });
        // Wait to know if the message has been sent or not
        if (m_promiseSender.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
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
        m_promiseReceiver = std::promise<void>();

        (!filter.empty()) ? setSubscriptions(filter, messageListener) : setSubscriptions(address, messageListener);

        m_connection.work_queue().add([=]() {
            m_connection.open_receiver(address, proton::receiver_options().auto_accept(true));
        });

        if (m_promiseReceiver.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
            deliveryState = DeliveryState::Accepted;
        }
    }
    return deliveryState;
}

void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
{
    std::lock_guard<std::mutex> lock(m_lock);
    logDebug("Message arrived: {}", proton::to_string(msg));
    delivery.accept();
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection) {
        std::string key = msg.address();
        if (!msg.correlation_id().empty() && msg.reply_to().empty()) {
            key = proton::to_string(msg.correlation_id());
        }

        if (auto it {m_subscriptions.find(key)}; it != m_subscriptions.end()) {
            // Message listener called by qpid-proton library

            // NOTE: Need to execute the callback in another pool of thread. The callback
            // treatment is executing in the same worker as sender/receiver and hang the response.
            // TODO: Don't work with another proton worker
            //m_workQueue.add(proton::make_work(it->second, amqpMsg));
            m_pool->offload(std::bind(it->second, amqpMsg));
        }
        else {
            logWarn("No message listener checked in for: {}", key);
        }
    } else {
        // Connection not set
        logError("Nothing to do, connection object not set");
    }
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

DeliveryState AmqpClient::unreceive(const std::string& address)
{
    auto deliveryState = DeliveryState::Unavailable;
    std::lock_guard<std::mutex> lock(m_lockMain);

    // TODO: Remove filter doesn't work !!!
    // Remove first address in subscriptions list
    unsetSubscriptions(address);

    // Then find recever with input address and close it
    bool isFound = false;
    auto receivers = m_connection.receivers();
    for (auto receiver : receivers) {
        if (receiver.source().address() == address) {
            isFound = true;
            receiver.close();
            deliveryState = DeliveryState::Accepted;
            // TODO: Wait stop receiver dosn't work: exception !!!!
            //m_connection.work_queue().add([&]() { receiver.close(); });
            /*m_promiseReceiver = std::promise<void>();
            m_connection.work_queue().add([&]() { receiver.close(); });
            if (m_promiseReceiver.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
                logDebug("Receiver closed for {}", address);
                deliveryState = DeliveryState::Accepted;
            } else {
                logError("Error on unreceive for {}, timeout reached", address);
            } */
        }
    }
    if (!isFound)  {
        logWarn("Unable to unreceive address {}: not present", address);
    }
    return deliveryState;
}

void AmqpClient::close()
{
    std::lock_guard<std::mutex> lock(m_lock);
    m_deconnectPromise = std::promise<void>();
    if (m_connection && m_connection.active()) {
        m_connection.work_queue().add([=]() { m_connection.close(); });

        auto deconnectFuture = m_deconnectPromise.get_future();
        if (deconnectFuture.wait_for(TIMEOUT) == std::future_status::timeout) {
            logError("*** AmqpClient::close De-connection timeout reached");
        }
    }
}

} // namespace fty::messagebus2::amqp
