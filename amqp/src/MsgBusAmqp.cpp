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

        for (const auto& [key, receiver] : m_subScriptions) {
            logDebug("Cleaning: {}...", key);
            receiver->close();
        }
        logDebug("{} cleaned", m_clientName);
    }
}

fty::Expected<void> MsgBusAmqp::connect()
{
    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    try {
        m_amqpClient = std::make_shared<AmqpClient>(m_endpoint);
        std::thread thrdSender([=]() {
            proton::container(*m_amqpClient).run();
        });
        thrdSender.detach();

        if (m_amqpClient->connected() != ComState::Ok) {
            return fty::unexpected(to_string(m_amqpClient->connected()));
        }
    } catch (const std::exception& e) {
        logError("Unexpected error: {}", e.what());
        return fty::unexpected(to_string(ComState::ConnectFailed));
    }
    return {};
}

bool MsgBusAmqp::isServiceAvailable()
{
    return (m_amqpClient && (m_amqpClient->connected() == ComState::Ok));
}

fty::Expected<void> MsgBusAmqp::receive(const Address& address, MessageListener messageListener, const std::string& filter)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(to_string(DeliveryState::Unavailable));
    }

    auto        receiver = std::make_shared<AmqpClient>(m_endpoint);
    std::thread thrd([=]() {
        proton::container(*receiver).run();
    });
    auto        received = receiver->receive(address, filter, messageListener);
    m_subScriptions.emplace(address, receiver);
    thrd.detach();

    if (received != DeliveryState::Accepted) {
        logError("Message receive (Rejected)");
        return fty::unexpected(to_string(DeliveryState::Rejected));
    }
    return {};
}

fty::Expected<void> MsgBusAmqp::unreceive(const Address& address)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(to_string(DeliveryState::Unavailable));
    }

    if (auto it{m_subScriptions.find(address)}; it != m_subScriptions.end()) {
        m_subScriptions.at(address)->unreceive();
        m_subScriptions.erase(address);
        logTrace("Unsubscribed for: '{}'", address);
    } else {
        logError("Unsubscribed '{}' (Rejected)", address);
        return fty::unexpected(to_string(DeliveryState::Rejected));
    }
    return {};
}

fty::Expected<void> MsgBusAmqp::send(const Message& message)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(to_string(DeliveryState::Unavailable));
    }

    logDebug("Sending message {}", message.toString());
    proton::message msgToSend = getAmqpMessage(message);

    auto        sender = AmqpClient(m_endpoint);
    std::thread thrd([&]() {
        proton::container(sender).run();
    });
    auto        msgSent = sender.send(msgToSend);
    sender.close();
    thrd.join();

    if (msgSent != DeliveryState::Accepted) {
        logError("Message sent (Rejected)");
        return fty::unexpected(to_string(msgSent));
    }

    logDebug("Message sent (Accepted)");
    return {};
}

fty::Expected<Message> MsgBusAmqp::request(const Message& message, int receiveTimeOut)
{
    try {
        if (!isServiceAvailable()) {
            logDebug("Service not available");
            return fty::unexpected(to_string(DeliveryState::Unavailable));
        }

        proton::message msgToSend = getAmqpMessage(message);

        AmqpClient  requester(m_endpoint);
        std::thread thrd([&]() {
            proton::container(requester).run();
        });
        requester.receive(msgToSend.reply_to(), proton::to_string(msgToSend.correlation_id()));
        auto msgSent = send(message);
        if (!msgSent) {
            return fty::unexpected(to_string(DeliveryState::Rejected));
        }

        MessagePointer response       = std::make_shared<proton::message>();
        bool           messageArrived = requester.tryConsumeMessageFor(response, receiveTimeOut);

        requester.close();
        thrd.join();

        if (!messageArrived) {
            logError("No message arrive in time!");
            return fty::unexpected(to_string(DeliveryState::Timeout));
        }

        logDebug("Message arrived ({})", proton::to_string(*response));
        return Message{getMetaData(*response), response->body().empty() ? std::string{} : proton::to_string(response->body())};
    } catch (std::exception& e) {
        return fty::unexpected(e.what());
    }
}

} // namespace fty::messagebus::amqp
