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

#include "MsgBusAmqp.h"
#include "MsgBusAmqpUtils.h"
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/utils.h>
#include <fty_log.h>

namespace fty::messagebus::amqp {

using namespace fty::messagebus;
using proton::receiver_options;
using proton::source_options;

MsgBusAmqp::~MsgBusAmqp()
{
    // Cleaning amqp ressources
    if (isServiceAvailable()) {
        logDebug("Cleaning Amqp ressources for: {}", m_clientName);
        for (const auto& [key, receiver] : m_clientHandler) {
            logDebug("Cleaning: {}...", key);
            receiver->close();
        }
        logDebug("{} cleaned", m_clientName);
    }
}

fty::Expected<void, ComState> MsgBusAmqp::connect()
{
    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    try {
        auto amqpClient = std::make_shared<AmqpClient>(m_endpoint);
        std::thread thrdSender([=]() {
            proton::container(*amqpClient).run();
        });
        thrdSender.detach();
        setHandler(m_endpoint, amqpClient);

        if (amqpClient->connected() != ComState::Connected) {
            return fty::unexpected(amqpClient->connected());
        }
    } catch (const std::exception& e) {
        logError("Unexpected error: {}", e.what());
        return fty::unexpected(ComState::ConnectFailed);
    }
    return {};
}

bool MsgBusAmqp::isServiceAvailable()
{
    bool serviceAvailable = false;
    if (auto it{m_clientHandler.find(m_endpoint)}; it != m_clientHandler.end()) {
        serviceAvailable = (m_clientHandler.at(m_endpoint)->connected() == ComState::Connected);
    }
    return serviceAvailable;
}

void MsgBusAmqp::setHandler(const Endpoint& endPoint, const AmqpClientPointer& amqpClient)
{
  std::lock_guard<std::mutex> lock(m_lock);
  m_clientHandler.emplace(endPoint, amqpClient);
}

fty::Expected<void, DeliveryState> MsgBusAmqp::receive(const Address& address, MessageListener messageListener, const std::string& filter)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    auto        receiver = std::make_shared<AmqpClient>(m_endpoint);
    std::thread thrd([=]() {
        proton::container(*receiver).run();
    });
    auto        received = receiver->receive(address, filter, messageListener);
    thrd.detach();

    if (received != DeliveryState::Accepted) {
        logError("Message receive (Rejected)");
        return fty::unexpected(DeliveryState::Rejected);
    }
    setHandler(address, receiver);
    return {};
}

fty::Expected<void, DeliveryState> MsgBusAmqp::unreceive(const Address& address)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    std::lock_guard<std::mutex> lock(m_lock);
    if (auto it{m_clientHandler.find(address)}; it != m_clientHandler.end()) {
        m_clientHandler.at(address)->unreceive();
        m_clientHandler.erase(address);
        logTrace("Unsubscribed for: '{}'", address);
    } else {
        logWarn("Unsubscribed '{}' (Rejected)", address);
        return fty::unexpected(DeliveryState::Rejected);
    }
    return {};
}

fty::Expected<void, DeliveryState> MsgBusAmqp::send(const Message& message)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    logDebug("Sending message ...");
    proton::message msgToSend = getAmqpMessage(message);

    /* auto        sender = AmqpClient(m_endpoint);
    std::thread thrd([&]() {
        proton::container(sender).run();
    });
    auto        msgSent = sender.send(msgToSend);
    sender.close();
    thrd.join(); */

    auto        msgSent = m_clientHandler.at(m_endpoint)->send(msgToSend);

    if (msgSent != DeliveryState::Accepted) {
        logError("Message sent (Rejected)");
        return fty::unexpected(msgSent);
    }

    logDebug("Message sent (Accepted)");
    return {};
}

fty::Expected<Message, DeliveryState> MsgBusAmqp::request(const Message& message, int timeoutInSeconds)
{
    try {
        if (!isServiceAvailable()) {
            logDebug("Service not available");
            return fty::unexpected(DeliveryState::Unavailable);
        }

        logDebug("Synchronous request and checking answer until {} second(s)...", timeoutInSeconds);
        proton::message msgToSend = getAmqpMessage(message);
        // Promise and future to check if the answer arrive constraint by timeout.
        auto promiseSyncRequest = std::promise<Message>();

        Message reply;
        bool msgArrived = false;
        MessageListener syncMessageListener = [&](const Message& replyMessage) {
          promiseSyncRequest.set_value(replyMessage);
        };

        auto msgReceived = m_clientHandler.at(m_endpoint)->receive(msgToSend.reply_to(), proton::to_string(msgToSend.correlation_id()), syncMessageListener);
        //auto msgReceived = receive(msgToSend.reply_to(), syncMessageListener, proton::to_string(msgToSend.correlation_id()));
        /* if (!msgReceived) {
            return fty::unexpected(DeliveryState::Aborted);
        } */

        auto msgSent = send(message);
        if (!msgSent) {
            logError("Issue on send message");
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
        //auto unreceived = m_clientHandler.at(m_endpoint)->unreceive();
        //auto unreceived = unreceive(msgToSend.reply_to());
        /* if (!unreceived) {
            logWarn("Issue on unreceive");
        } */

        if (!msgArrived) {
            logError("No message arrived within {} seconds!", timeoutInSeconds);
            return fty::unexpected(DeliveryState::Timeout);
        }

        return futureSynRequest.get();
    } catch (std::exception& e) {
        logError("Exception in synchronous request: {}", e.what());
        return fty::unexpected(DeliveryState::Aborted);
    }
}

} // namespace fty::messagebus::amqp
