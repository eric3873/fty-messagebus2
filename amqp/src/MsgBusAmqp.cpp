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
        logDebug("Cleaning amqp client connection");
        m_amqpClient->close();
        logDebug("{} cleaned", m_clientName);
    }
}

fty::Expected<void, ComState> MsgBusAmqp::connect()
{
    logDebug("Connecting for {} to {} ...", m_clientName, m_endpoint);
    try {
        m_amqpClient = std::make_shared<AmqpClient>(m_endpoint);
        std::thread thrdSender([=]() {
            proton::container(*m_amqpClient).run();
        });
        thrdSender.detach();

        if (m_amqpClient->connected() != ComState::Ok) {
            return fty::unexpected(m_amqpClient->connected());
        }
    } catch (const std::exception& e) {
        logError("Unexpected error: {}", e.what());
        return fty::unexpected(ComState::ConnectFailed);
    }
    return {};
}

bool MsgBusAmqp::isServiceAvailable()
{
    return (m_amqpClient && (m_amqpClient->connected() == ComState::Ok));
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
    m_subScriptions.emplace(address, receiver);
    thrd.detach();

    if (received != DeliveryState::Accepted) {
        logError("Message receive (Rejected)");
        return fty::unexpected(DeliveryState::Rejected);
    }
    return {};
}

fty::Expected<void, DeliveryState> MsgBusAmqp::unreceive(const Address& address)
{
    if (!isServiceAvailable()) {
        logDebug("Service not available");
        return fty::unexpected(DeliveryState::Unavailable);
    }

    if (auto it{m_subScriptions.find(address)}; it != m_subScriptions.end()) {
        m_subScriptions.at(address)->unreceive();
        m_subScriptions.erase(address);
        logTrace("Unsubscribed for: '{}'", address);
    } else {
        logError("Unsubscribed '{}' (Rejected)", address);
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

    logDebug("Sending message {}", message.toString());
    //logDebug("Sending message ...");
    proton::message msgToSend = getAmqpMessage(message);
    //logDebug("1");

    auto        sender = AmqpClient(m_endpoint);
    std::thread thrd([&]() {
        proton::container(sender).run();
    });
    //logDebug("2");
    auto        msgSent = sender.send(msgToSend);
    //logDebug("3");
    sender.close();
    //logDebug("4");
    thrd.join();
    //logDebug("5");

    if (msgSent != DeliveryState::Accepted) {
        logError("Message sent (Rejected)");
        return fty::unexpected(msgSent);
    }

    logDebug("Message sent (Accepted)");
    return {};
}

fty::Expected<Message, DeliveryState> MsgBusAmqp::request(const Message& message, int receiveTimeOut)
{
    //std::future<int> result1(std::async(func2));
    auto f = std::async(std::launch::async, fty::messagebus::amqp::MsgBusAmqp::func2, 8);
    logWarn("result is {}", f.get());
    try {
        if (!isServiceAvailable()) {
            logDebug("Service not available");
            return fty::unexpected(DeliveryState::Unavailable);
        }

        proton::message msgToSend = getAmqpMessage(message);

        //auto promiseSyncRequest = std::promise<proton::message>();

        auto promiseSyncRequest = std::promise<Message>();

        Message reply;
        bool messageArrived = false;
        MessageListener func1 = [&](const Message& message2) {
          logDebug("Message arrived: '{}'", message2.toString());
          promiseSyncRequest.set_value(message2);
          //reply = std::move(message2);
          //messageArrived = true;
        };

        /* Message reply2 = [=](const Message& message2) {
          logDebug("Message arrived: '{}'", message2.toString());
          return message2;
          //messageArrived = true;
        }; */


        auto received = receive(msgToSend.reply_to(), func1, proton::to_string(msgToSend.correlation_id()));

        // AmqpClient  requester(m_endpoint);
        // std::thread thrd([&]() {
        //     proton::container(requester).run();
        // });
        // auto received = requester.receive(msgToSend.reply_to(), proton::to_string(msgToSend.correlation_id()));
        // if (received != DeliveryState::Accepted) {
        //     return fty::unexpected(DeliveryState::Aborted);
        // }
        // // Let the time to the receiver to be in the place

        auto msgSent = send(message);
        if (!msgSent) {
            return fty::unexpected(DeliveryState::Aborted);
        }


        auto futureSynRequest = promiseSyncRequest.get_future();
        if (futureSynRequest.wait_for(std::chrono::seconds(receiveTimeOut)) != std::future_status::timeout) {
          messageArrived = true;
        }

        //std::this_thread::sleep_for(std::chrono::seconds(2));
        //MessagePointer response       = std::make_shared<proton::message>();
        //bool           messageArrived = true;
        //bool           messageArrived = m_amqpClient->tryConsumeMessageFor(response, receiveTimeOut);
        //bool           messageArrived = m_subScriptions.at(msgToSend.reply_to())->tryConsumeMessageFor(response, receiveTimeOut);
        /* requester.close();
        thrd.join(); */
        unreceive(msgToSend.reply_to());

        if (!messageArrived) {
            logError("No message arrive in time!");
            return fty::unexpected(DeliveryState::Timeout);
        }

        //logDebug("Message arrived ({})", proton::to_string(*response));
        //return Message();
        //return Message{getMetaData(*response), response->body().empty() ? std::string{} : proton::to_string(response->body())};
        return futureSynRequest.get();
    } catch (std::exception& e) {
        logError("Exception in amqp receive: {}", e.what());
        return fty::unexpected(DeliveryState::Aborted);
    }
}

} // namespace fty::messagebus::amqp
