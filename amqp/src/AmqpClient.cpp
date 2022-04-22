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
      opts.idle_timeout(proton::duration(5000));
      return opts;
  }

} // namespace

namespace fty::messagebus::amqp {
using namespace fty::messagebus;
using MessageListener = fty::messagebus::MessageListener;

static auto constexpr TIMEOUT = std::chrono::seconds(4);

AmqpClient::AmqpClient(const Endpoint& url)
    : m_url(url)
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

void AmqpClient::on_sender_open(proton::sender& sender)
{
    sender.send(m_message);
    m_promiseSender.set_value();
    //sender.session().close();
    logDebug("Message sent");
}

void AmqpClient::on_receiver_open(proton::receiver& receiver)
{
    logDebug("Waiting any message on target address: {}", receiver.source().address());
    // Record receiver to have the possibility to unreceive it (i.e. close it)
    m_receiver = receiver;
    m_promiseReceiver.set_value();
}

void AmqpClient::on_receiver_close(proton::receiver&)
{
  logDebug("on_receiver_close");
  //m_promiseReceiver.set_value();
  m_promiseSession.set_value();
}

void AmqpClient::on_session_open(proton::session& session)
{
  //logDebug("on_session_close {} - {}", m_connection.container_id(), session.container().id());
  logDebug("on_session_open {}", session.container().id());
}

void AmqpClient::on_session_close(proton::session& session)
{
  //logDebug("on_session_close {} - {}", m_connection.container_id(), session.container().id());
  //logDebug("on_session_close {}", session.container().id());
  logDebug("on_session_close ");
  //m_promiseSession.set_value();
}

void AmqpClient::on_error(const proton::error_condition& error)
{
    logError("Protocol error: {}", error.what());
}

void AmqpClient::on_transport_error(proton::transport& transport)
{
    logError("Transport error: {}", transport.error().what());
    m_communicationState = ComState::Lost;
}

void AmqpClient::resetPromise()
{
    std::unique_lock<std::mutex> lock(m_mutex);
    logDebug("Reset all promise");
    m_connectPromise  = std::promise<fty::messagebus::ComState>();
    m_promiseSender   = std::promise<void>();
    m_promiseReceiver = std::promise<void>();
}

ComState AmqpClient::connected()
{
    if ((m_communicationState == ComState::Unknown) || (m_communicationState == ComState::Lost)) {

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
    return m_communicationState;
}

DeliveryState AmqpClient::send(const proton::message& msg)
{
    auto deliveryState = DeliveryState::Rejected;
    if (connected() == ComState::Connected) {
        m_promiseSender = std::promise<void>();
        logDebug("Sending message to {} ...", msg.to());
        m_message.clear();
        m_message = msg;

        /* proton::session session = m_connection.open_session();
        session.open_sender(msg.to()); */

        m_connection.work_queue().add([=]() {
            //m_connection.open_sender(msg.to());
            /*proton::session session = */
        //m_connection.open_session().open_sender(msg.to());
        m_connection.default_session().open_sender(msg.to());
            /* session.open_sender(msg.to()) */;
        });

        // Wait to know if the message has been sent or not
        if (m_promiseSender.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
            deliveryState = DeliveryState::Accepted;
        }
    }
    return deliveryState;
}

DeliveryState AmqpClient::receive(const Address& address, const std::string& filter, MessageListener messageListener)
{
    auto deliveryState = DeliveryState::Rejected;
    if (connected() == ComState::Connected) {
        logDebug("Set receiver to wait message(s) from {} ...", address);
        m_promiseReceiver = std::promise<void>();

        (!filter.empty()) ? setSubscriptions(filter, messageListener) : setSubscriptions(address, messageListener);

        m_connection.work_queue().add([=]() {
            m_connection.default_session().open_receiver(address, proton::receiver_options().auto_accept(true));
        });

        if (m_promiseReceiver.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
            deliveryState = DeliveryState::Accepted;
        }
    }
    return deliveryState;
}

void AmqpClient::on_message(proton::delivery& delivery, proton::message& msg)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    logDebug("Message arrived: {}", proton::to_string(msg));
    delivery.accept();
    Message amqpMsg(getMetaData(msg), msg.body().empty() ? std::string{} : proton::to_string(msg.body()));

    if (m_connection) {
        std::string key = msg.address();
        if (!msg.correlation_id().empty() && msg.reply_to().empty()) {
            key = proton::to_string(msg.correlation_id());
        }
        if (!m_subscriptions.first.empty() && !m_subscriptions.first.compare(key) && m_subscriptions.second != nullptr) {
          m_connection.work_queue().add(proton::make_work(m_subscriptions.second, amqpMsg));
        } else {
            logWarn("No message listener checked in for: {}", key);
        }
    } else {
        // Connection not set
        logError("Nothing to do, connection object not set");
    }
}

void AmqpClient::setSubscriptions(const Address& address, MessageListener messageListener)
{
    std::unique_lock<std::mutex> lock(m_mutex);
    if (!address.empty() && messageListener) {
        m_subscriptions = std::make_pair(address, messageListener);
    } else {
        logWarn("Subscriptions skipped, call back information not filled!");
    }
}

DeliveryState AmqpClient::unreceive()
{
    //std::unique_lock<std::mutex> lock(m_mutex);
    //m_promiseReceiver = std::promise<void>();
    m_promiseSession = std::promise<void>();
    auto deliveryState = DeliveryState::Unavailable;
    if (m_receiver) {
        if (m_receiver.active()) {// && !m_receiver.session().active()) {
          logDebug("on unreceive() {}", m_receiver.session().container().id());
          deliveryState = DeliveryState::Accepted;
          //m_receiver.close();
          for (proton::receiver_iterator i = m_connection.receivers().begin(); i != m_connection.receivers().end(); ++i) {
            i->close();
          }
          //m_receiver.session().close();
          logDebug("on unreceive()2");
          if (m_promiseSession.get_future().wait_for(TIMEOUT) != std::future_status::timeout) {
            logDebug("Receiver closed for {}", m_receiver.source().address());
          } else {
            logError("Error on receiver close for {}, timeout reached", m_receiver.source().address());
          }
        }
        std::unique_lock<std::mutex> lock(m_mutex);
        m_subscriptions = {};
    }
    return deliveryState;
}

void AmqpClient::close()
{
    unreceive();
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_connection && m_connection.active()) {
        m_connection.close();
        logDebug("Connection Closed");
    }
}

} // namespace fty::messagebus::amqp
