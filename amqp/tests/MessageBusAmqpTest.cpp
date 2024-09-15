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

#include <catch2/catch.hpp>
#include <fty/messagebus2/Message.h>
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty/messagebus2/amqp/MessageBusAmqp.h>
#include <fty/messagebus2/utils.h>
#include "../src/MsgBusAmqpUtils.h"
#include "../src/AmqpClient.h"
#include <iostream>
#include <mutex>
#include <thread>
#include <chrono>
#include <future>
#include <chrono>

#if defined(EXTERNAL_SERVER_FOR_TEST)
static constexpr auto AMQP_SERVER_URI {"x.x.x.x:5672"};
#else
static constexpr auto AMQP_SERVER_URI {"amqp:tcp:127.0.0.1:5672"};
#endif

using namespace fty::messagebus2;
using namespace fty::messagebus2::utils;
using namespace std::chrono_literals;

auto constexpr ONE_SECOND           = std::chrono::seconds(1);
auto constexpr TWO_SECONDS          = std::chrono::seconds(2);
auto constexpr SYNC_REQUEST_TIMEOUT = 10; // in second
// TBD: Test not passing CI with 100 threads (Random unexpected exception)
auto constexpr NB_THREAD_MULTI      = 10; // nb of threads for multi tests

static const std::string QUERY        = "query";
static const std::string QUERY_2      = "query2";
static const std::string OK           = ":OK";
static const std::string QUERY_AND_OK = QUERY + OK;
static const std::string RESPONSE_2   = QUERY_2 + OK;

// Class for test message receiption
class MsgReceived
{
private:
    // Mutex
    std::mutex                            m_lock;
    std::shared_ptr<amqp::MessageBusAmqp> m_msgBusReplyer;

public:
    int receiver;
    int replyer;

    MsgReceived() :
        receiver(0),
        replyer(0)
    {
        m_msgBusReplyer = std::make_shared<amqp::MessageBusAmqp>("TestCase", AMQP_SERVER_URI);
        REQUIRE(m_msgBusReplyer->connect());
    }

    ~MsgReceived() = default;

    void reset()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        receiver = 0;
        replyer  = 0;
    }

    void incReceiver()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        receiver++;
    }

    void incReplyer()
    {
        std::lock_guard<std::mutex> lock(m_lock);
        replyer++;
    }

    bool assertValue(const int expected)
    {
        std::cout << "receiver: " << receiver << " <> " <<  expected << " replyer: " << replyer << " <> " << expected << std::endl;
        return (receiver == expected && replyer == expected);
    }

    bool isReceived(const int expected)
    {
        return (receiver == expected);
    }

    int getReceived()
    {
        return receiver;
    }

    void messageListener(const Message& message)
    {
        incReceiver();
        std::cout << "messageListener: Message arrived " << message.toString() << std::endl;
    }

    void replyerAddOK(const Message& message)
    {
        incReplyer();
        // Build the response
        std::cout << "replyerAddOK:" << message.toString() << std::endl;
        auto response = message.buildReply(message.userData() + OK);

        if (!response) {
            std::cerr << response.error() << std::endl;
        }

        // send the response
        auto msgSent = m_msgBusReplyer->send(response.value());
        if (!msgSent) {
            FAIL(to_string(msgSent.error()));
        }
        else {
            std::cout << "replyerAddOK Send OK " << replyer << std::endl;
        }
    }

    void replyerEcho(const Message& message)
    {
        incReplyer();
        // Build the response
        auto response = message.buildReply(message.userData());

        if (!response) {
            std::cerr << response.error() << std::endl;
        }

        // send the response
        auto msgSent = m_msgBusReplyer->send(response.value());
        if (!msgSent) {
            FAIL(to_string(msgSent.error()));
        }
        else {
            std::cout << "replyerAddEcho Send OK " << replyer << std::endl;
        }
    }
};

//----------------------------------------------------------------------
// Test case
//----------------------------------------------------------------------

TEST_CASE("Identity", "[amqp][identity]")
{
    auto msgBus = amqp::MessageBusAmqp("IdentityTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBus.clientName() == "IdentityTestCase");
    REQUIRE(msgBus.identity() == amqp::BUS_IDENTITY);
}

TEST_CASE("queue", "[amqp][request]")
{
    SECTION("Send request sync timeout reached")
    {
        MsgReceived msgReceived;
        std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";
        auto        msgBusRequesterSync  = amqp::MessageBusAmqp("SyncRequesterTimeOutTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Send synchronous request
        Message request = Message::buildRequest(
            "SyncRequesterTimeOutTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);

        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

        auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
        REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);

        REQUIRE(msgBusReplyer.unreceive(request.to()));

        auto noReplyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
        REQUIRE(!noReplyMsg);
        REQUIRE(noReplyMsg.error() == DeliveryState::Timeout);

        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
        auto withReplyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
        REQUIRE(withReplyMsg.value().userData() == QUERY_AND_OK);

        REQUIRE(msgBusReplyer.unreceive(request.to()));
    }

    SECTION("Send sync request")
    {
        MsgReceived msgReceived;
        std::string syncTestQueue       = "queue://test.message.sync.";
        auto        msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Build synchronous request and set all receiver
        Message request = Message::buildRequest("RequestTestCase", syncTestQueue + "request", "TEST", syncTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);
        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

        auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
        REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);

        REQUIRE(msgBusReplyer.unreceive(request.to()));
    }

    std::this_thread::sleep_for(ONE_SECOND);

    SECTION("Send async request")
    {
        MsgReceived msgReceived;
        std::string asyncTestQueue  = "queue://test.message.async.";
        auto        msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequester.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Build asynchronous request and set all receiver
        Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
        REQUIRE(msgBusRequester.receive(
            request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
            request.correlationId()));

        for (int i = 0; i < 3; i++) {
            REQUIRE(msgBusRequester.send(request));
            std::this_thread::sleep_for(ONE_SECOND);
            CHECK(msgReceived.assertValue(i + 1));
        }

        REQUIRE(msgBusReplyer.unreceive(request.to()));
        REQUIRE(msgBusRequester.unreceive(request.replyTo(), request.correlationId()));
    }

    SECTION("Send")
    {
        MsgReceived msgReceived;
        std::string sendTestQueue = "queue://test.message.send";

        auto msgBus = amqp::MessageBusAmqp("MessageRecieverSendTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBus.connect());

        auto msgBusSender = amqp::MessageBusAmqp("MessageSenderSendTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusSender.connect());

        REQUIRE(msgBusSender.receive(sendTestQueue, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        // Send message on queue
        Message msg = Message::buildMessage("MqttMessageTestCase", sendTestQueue, "TEST", QUERY);

        for (int i = 0; i < 3; i++) {
            REQUIRE(msgBusSender.send(msg));
            std::this_thread::sleep_for(ONE_SECOND);
            CHECK(msgReceived.isReceived(i + 1));
        }

        REQUIRE(msgBusSender.unreceive(sendTestQueue));
    }
}

TEST_CASE("test request/reply synch with temporary queue", "[amqp][test]")
{
    for (int i = 0; i < 10; i++) {
        MsgReceived msgReceived;
        std::string syncTestQueue       = "queue://test2.message.sync.";
        auto        msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Build synchronous request and set all receiver
        Message request = Message::buildRequest("RequestTestCase", syncTestQueue + "request", "TEST", "", QUERY, {}, SYNC_REQUEST_TIMEOUT);
        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

        auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
        REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);

        REQUIRE(msgBusReplyer.unreceive(request.to()));
    }
}

TEST_CASE("test request/reply asynch with temporary queue", "[amqp][test]")
{
    for (int i = 0; i < 10; i++) {
        MsgReceived msgReceived;
        std::string asyncTestQueue  = "queue://test2.message.async.";
        auto        msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequester.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Build asynchronous request and set all receiver
        Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", "", QUERY);
        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
        REQUIRE(
            msgBusRequester
                .receive("", std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1), request.correlationId()));

        REQUIRE(msgBusRequester.send(request));
        std::this_thread::sleep_for(std::chrono::milliseconds(2000));
        CHECK(msgReceived.assertValue(1));

        REQUIRE(msgBusRequester.unreceive(request.replyTo(), request.correlationId()));
        REQUIRE(msgBusReplyer.unreceive(asyncTestQueue + "request"));
    }
}

TEST_CASE("test send different message", "[amqp][test]")
{
    MsgReceived msgReceived;
    std::string asyncTestQueue  = "queue://test.message.async.";
    auto        msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    REQUIRE(msgBusReplyer.receive(asyncTestQueue + "request", std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    for (int i = 0; i < 10; i++) {
        Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
        REQUIRE(msgBusRequester.receive(
            request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
            request.correlationId()));
        REQUIRE(msgBusRequester.send(request));
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        CHECK(msgReceived.assertValue(i + 1));
        REQUIRE(msgBusRequester.unreceive(request.replyTo(), request.correlationId()));
    }
    REQUIRE(msgBusReplyer.unreceive(asyncTestQueue + "request"));
}

TEST_CASE("test send same message", "[amqp][test]")
{
    MsgReceived msgReceived;
    std::string asyncTestQueue  = "queue://test.message.async.";
    auto        msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    REQUIRE(msgBusReplyer.receive(asyncTestQueue + "request", std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);

    for (int i = 0; i < 10; i++) {
        REQUIRE(msgBusRequester.receive(
            request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
            request.correlationId()));
        REQUIRE(msgBusRequester.send(request));
        //std::this_thread::sleep_for(std::chrono::milliseconds(200));
        std::this_thread::sleep_for(std::chrono::seconds(1));
        CHECK(msgReceived.assertValue(i + 1));
        REQUIRE(msgBusRequester.unreceive(request.replyTo(), request.correlationId()));
    }

    REQUIRE(msgBusReplyer.unreceive(asyncTestQueue + "request"));
}

TEST_CASE("test filter with bad receiver", "[amqp][filter]")
{
    MsgReceived msgReceived;
    std::string testQueue1       = "queue://test.message.queue.1.";
    std::string testQueue2       = "queue://test.message.queue.2.";
    std::string badCorrelationId = "0123456789";

    auto msgBusRequester1 = amqp::MessageBusAmqp("RequesteTestCase1", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester1.connect());

    auto msgBusRequester2 = amqp::MessageBusAmqp("RequesteTestCase2", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester2.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request = Message::buildRequest("AsyncRequestTestCase", testQueue1 + "request", "TEST", testQueue1 + "reply", QUERY);
    REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
    // Set bad receiver
    REQUIRE(msgBusRequester1.receive(
        request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1), /*request.correlationId()*/badCorrelationId));

    REQUIRE(msgBusRequester1.send(request));
    std::this_thread::sleep_for(std::chrono::seconds(1));
    // Don't receive reply
    CHECK(!msgReceived.assertValue(1));

    REQUIRE(msgBusRequester1.unreceive(request.replyTo(), /*request.correlationId()*/ badCorrelationId));

    // Set good receiver
    REQUIRE(msgBusRequester2.receive(
        request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
        request.correlationId()));
    std::this_thread::sleep_for(std::chrono::seconds(10));

    REQUIRE(msgBusReplyer.unreceive(request.to()));
    REQUIRE(msgBusRequester2.unreceive(request.replyTo(), request.correlationId()));

    // Receive reply
    CHECK(msgReceived.assertValue(1));
}

TEST_CASE("multi synch filter test", "[amqp][multi][synch][filter]")
{
    bool noThrow = true;
    try {
        MsgReceived msgReceived;
        std::string testMultiQueue = "queue://test.message.multi.";
        std::string requestQueue   = testMultiQueue + "request";
        std::string replyQueue     = testMultiQueue + "reply";

        auto msgBusReplyer = amqp::MessageBusAmqp("ReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());
        REQUIRE(msgBusReplyer.receive(requestQueue, std::bind(&MsgReceived::replyerEcho, std::ref(msgReceived), std::placeholders::_1)));

        auto msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        // Synchronous
        auto sendSynch = [&](int num) {
            try {
                std::cout << "Debut thread " << num << std::endl;

                // Build synchronous request and set all receiver
                Message request = Message::buildRequest("RequestTestCase", requestQueue, "TEST", replyQueue, std::to_string(num), {}, SYNC_REQUEST_TIMEOUT);

                auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
                REQUIRE(replyMsg);
                REQUIRE(replyMsg.value().userData() == std::to_string(num));

                std::cout << "Fin thread " << num << std::endl;
            } catch (std::exception& e) {
                std::cout << "EXECPTION THREAD " << num << ": " << e.what() << std::endl;
                noThrow = false;
            }
            //std::this_thread::sleep_for(1000ms);
        };

        int nbThread = NB_THREAD_MULTI;
        std::vector<std::thread> myThreads;
        // for odd version
        for (int i = 1; i < nbThread; i += 2) {
            // Synch version
            myThreads.push_back(std::thread(sendSynch, i));
        }

        // for even version
        for (int i = 0; i < nbThread; i += 2) {
            // Synch version
            myThreads.push_back(std::thread(sendSynch, i));
        }

        int i = 0;
        for (auto& t : myThreads) {
            std::cout << "TEST " << i++ << std::endl;
            t.join();
        }
        REQUIRE(msgBusReplyer.unreceive(testMultiQueue + "request"));
    } catch (std::exception& e) {
        std::cout << "EXECPTION TEST: " << e.what() << std::endl;
        noThrow = false;
    }
    REQUIRE(noThrow);
}

TEST_CASE("multi asynch filter test", "[amqp][multi][asynch][filter]")
{
    bool noThrow = true;
    try {
        MsgReceived msgReceived;
        std::string testMultiQueue = "queue://test.message.multi";
        std::string requestQueue   = testMultiQueue + "request";
        std::string replyQueue     = testMultiQueue + "reply";

        auto msgBusReplyer = amqp::MessageBusAmqp("ReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());
        REQUIRE(msgBusReplyer.receive(requestQueue, std::bind(&MsgReceived::replyerEcho, std::ref(msgReceived), std::placeholders::_1)));

        auto msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequester.connect());

        // Asynchronous versions
        auto sendAsynch = [&](int num) {
            try {
                std::cout << "Debut thread " << num << std::endl;

                // Build asynchronous request and set all receiver
                Message request = Message::buildRequest(msgBusRequester.clientName(), requestQueue, "TEST", replyQueue, std::to_string(num));

                // Create the promise and future
                std::future<Message>  myFuture;
                std::promise<Message> myPromise;

                REQUIRE(msgBusRequester.receive(
                    request.replyTo(),
                    [&myPromise](const Message& m) {
                        myPromise.set_value(m);
                    },
                    request.correlationId()));
                myFuture = myPromise.get_future();

                auto replyMsg = msgBusRequester.send(request);
                REQUIRE(replyMsg);

                myFuture.wait();
                auto value = myFuture.get().userData();
                std::cout << "value=" << value << " <> " << num << std::endl;
                REQUIRE(value == std::to_string(num));
                REQUIRE(msgBusRequester.unreceive(request.replyTo(), request.correlationId()));

                std::cout << "Fin thread " << num << std::endl;
            } catch (std::exception& e) {
                std::cout << "EXECPTION: " << e.what() << std::endl;
                noThrow = false;
            }
            return 0;
        };

        int nbThread = NB_THREAD_MULTI;
        std::vector<std::thread> myThreads;
        // for odd version
        for (int i = 1; i < nbThread; i += 2) {
            // Asynch version
            myThreads.push_back(std::thread(sendAsynch, i));
        }

        // for even version
        for (int i = 0; i < nbThread; i += 2) {
            // Asynch version
            myThreads.push_back(std::thread(sendAsynch, i));
        }

        int i = 0;
        for (auto& t : myThreads) {
            std::cout << "TEST " << i++ << std::endl;
            t.join();
        }
        REQUIRE(msgBusReplyer.unreceive(testMultiQueue + "request"));
    } catch (std::exception& e) {
        std::cout << "EXECPTION TEST: " << e.what() << std::endl;
        noThrow = false;
    }
    REQUIRE(noThrow);
}


TEST_CASE("multi synch", "[amqp][multi][synch]")
{
    bool noThrow = true;
    try {
        MsgReceived msgReceived;
        std::string testMultiQueue = "queue://test.message.multi.";

        auto msgBusReplyer = amqp::MessageBusAmqp("ReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());
        REQUIRE(msgBusReplyer.receive(testMultiQueue + "request", std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

        // Synchronous version
        auto sendSynch = [&](int num) {
            try {
                // std::cout << "Debut thread " << num << std::endl;
                std::string requestQueue        = testMultiQueue + "request";
                std::string replyQueue          = testMultiQueue + "reply." + std::to_string(num);
                auto        msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase." + std::to_string(num), AMQP_SERVER_URI);
                REQUIRE(msgBusRequesterSync.connect());

                // Build synchronous request and set all receiver
                Message request =
                    Message::buildRequest("RequestTestCase", requestQueue, "TEST", replyQueue, QUERY, {}, SYNC_REQUEST_TIMEOUT);

                auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
                REQUIRE(replyMsg);
                REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);

                // std::cout << "Fin thread " << num << std::endl;
            } catch (std::exception& e) {
                std::cout << "EXECPTION THREAD " << num << ": " << e.what() << std::endl;
                noThrow = false;
            }
        };

        int                      nbThread = NB_THREAD_MULTI;
        std::vector<std::thread> myThreads;
        for (int i = 0; i < nbThread; i++) {
            // Synch version
            myThreads.push_back(std::thread(sendSynch, i));
        }

        int i = 0;
        for (auto& t : myThreads) {
            std::cout << "TEST " << i++ << std::endl;
            t.join();
        }
        REQUIRE(msgBusReplyer.unreceive(testMultiQueue + "request"));
    } catch (std::exception& e) {
        std::cout << "EXECPTION TEST: " << e.what() << std::endl;
        noThrow = false;
    }
    REQUIRE(noThrow);
}

TEST_CASE("multi asynch", "[amqp][multi][asynch]")
{
    bool noThrow = true;
    try {
        MsgReceived msgReceived;
        std::string testMultiQueue = "queue://test.message.multi";

        auto msgBusReplyer = amqp::MessageBusAmqp("ReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());
        REQUIRE(msgBusReplyer.receive(testMultiQueue + "request", std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

        // Asynchronous versions
        auto sendAsynch = [&](int num) {
            try {
                // std::cout << "Debut thread " << num << std::endl;
                std::string requestQueue = testMultiQueue + "request";
                std::string replyQueue   = testMultiQueue + "reply." + std::to_string(num);

                auto msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase" + std::to_string(num), AMQP_SERVER_URI);
                REQUIRE(msgBusRequester.connect());

                // Build asynchronous request and set all receiver
                Message request = Message::buildRequest(msgBusRequester.clientName(), requestQueue, "TEST", replyQueue, QUERY);

                // Create the promise and future
                std::future<Message>  myFuture;
                std::promise<Message> myPromise;

                REQUIRE(msgBusRequester.receive(
                    request.replyTo(),
                    [&myPromise](const Message& m) {
                        myPromise.set_value(m);
                    },
                    request.correlationId()));
                myFuture = myPromise.get_future();

                auto replyMsg = msgBusRequester.send(request);
                REQUIRE(replyMsg);

                myFuture.wait();
                REQUIRE(myFuture.get().userData() == QUERY_AND_OK);
                // REQUIRE(msgBusRequester.unreceive(request.replyTo()));

                std::cout << "Fin thread " << num << std::endl;
            } catch (std::exception& e) {
                std::cout << "EXECPTION: " << e.what() << std::endl;
                noThrow = false;
            }
            return 0;
        };

        int nbThread = NB_THREAD_MULTI;
        std::vector<std::thread> myThreads;
        for (int i = 0; i < nbThread; i++) {
            // Asynch version
            myThreads.push_back(std::thread(sendAsynch, i));
        }

        int i = 0;
        for (auto& t : myThreads) {
            std::cout << "TEST " << i++ << std::endl;
            t.join();
        }
        REQUIRE(msgBusReplyer.unreceive(testMultiQueue + "request"));
    } catch (std::exception& e) {
        std::cout << "EXECPTION TEST: " << e.what() << std::endl;
        noThrow = false;
    }
    REQUIRE(noThrow);
}

TEST_CASE("doublequeueSynch", "[amqp][request]")
{
    MsgReceived msgReceived;
    std::string asyncTestQueue = "queue://test.message.sameobject.";

    auto msgBusRequesterSync = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequesterSync.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request1 = Message::buildRequest("RequestTestCase", asyncTestQueue + "request1", "TEST", asyncTestQueue + "reply1", QUERY);
    REQUIRE(msgBusReplyer.receive(request1.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    Message request2 = Message::buildRequest("RequestTestCase", asyncTestQueue + "request2", "TEST", asyncTestQueue + "reply2", QUERY);
    REQUIRE(msgBusReplyer.receive(request2.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    std::thread sender1([&]() {
        auto replyMsg1 = msgBusRequesterSync.request(request1, SYNC_REQUEST_TIMEOUT);
        REQUIRE(replyMsg1.value().userData() == QUERY_AND_OK);
    });

    std::thread sender2([&]() {
        auto replyMsg2 = msgBusRequesterSync.request(request2, SYNC_REQUEST_TIMEOUT);
        REQUIRE(replyMsg2.value().userData() == QUERY_AND_OK);
    });
    sender1.join();
    sender2.join();
}

TEST_CASE("doublequeueAsynch", "[amqp][request]")
{
    MsgReceived msgReceived;
    std::string asyncTestQueue = "queue://test.message.sameobject.";

    auto msgBusRequesterAsync = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequesterAsync.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request1 = Message::buildRequest("RequestTestCase", asyncTestQueue + "request1", "TEST", asyncTestQueue + "reply1", QUERY);
    REQUIRE(msgBusReplyer.receive(request1.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    REQUIRE(msgBusRequesterAsync.receive(
        request1.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
        request1.correlationId()));

    Message request2 = Message::buildRequest("RequestTestCase", asyncTestQueue + "request2", "TEST", asyncTestQueue + "reply2", QUERY);
    REQUIRE(msgBusReplyer.receive(request2.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    REQUIRE(msgBusRequesterAsync.receive(
        request2.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
        request2.correlationId()));

    std::thread sender1([&]() {
        REQUIRE(msgBusRequesterAsync.send(request1));
    });

    std::thread sender2([&]() {
        REQUIRE(msgBusRequesterAsync.send(request2));
    });
    sender1.join();
    sender2.join();

    std::this_thread::sleep_for(3s);
    CHECK(msgReceived.assertValue(2));

    REQUIRE(msgBusRequesterAsync.unreceive(request1.replyTo(), request1.correlationId()));
    REQUIRE(msgBusRequesterAsync.unreceive(request2.replyTo(), request2.correlationId()));
}

TEST_CASE("queueWithSameObject", "[amqp][request]")
{
    MsgReceived msgReceived;
    std::string asyncTestQueue = "queue://test.message.sameobject.";

    auto msgBusRequesterSync = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequesterSync.connect());

    auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request = Message::buildRequest("RequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);

    {
        MsgReceived asyncMsgReceived;
        auto        msgBusRequesterAsync = amqp::MessageBusAmqp("AsyncRequesterTestCase2", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterAsync.connect());

        Message request2 = Message::buildRequest("RequestTestCase", asyncTestQueue + "request2", "TEST", asyncTestQueue + "reply2", QUERY);

        // TODO: TO FIX, need to have the same queue for synch and asynch
        //auto msgBusReplyer2 = amqp::MessageBusAmqp("AsyncReplyerTestCase2", AMQP_SERVER_URI);
        //REQUIRE(msgBusReplyer2.connect());
        //REQUIRE(msgBusReplyer2.receive(request2.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(asyncMsgReceived),
        //std::placeholders::_1)));
        REQUIRE(
            msgBusReplyer.receive(request2.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(asyncMsgReceived), std::placeholders::_1)));

        REQUIRE(msgBusRequesterAsync.receive(
            request2.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(asyncMsgReceived), std::placeholders::_1),
            request2.correlationId()));

        int i = 0;
        for (i = 0; i < 3; i++) {
            //REQUIRE(msgBusRequesterAsync.send(request));
            REQUIRE(msgBusRequesterAsync.send(request2));
            std::this_thread::sleep_for(2s);
            CHECK(asyncMsgReceived.assertValue(i + 1));
        }
        std::this_thread::sleep_for(ONE_SECOND);
        CHECK(asyncMsgReceived.isReceived(i));
        // TODO: TO FIX
        //REQUIRE(msgBusRequesterAsync.unreceive(request.replyTo()));
    }
  
    {
        for (int i = 0; i < 2; i++) {
            auto otherReplyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
            REQUIRE(otherReplyMsg.value().userData() == QUERY_AND_OK);
        }
    }
}

TEST_CASE("topic", "[amqp][pub]")
{
    SECTION("Publish subscribe")
    {
        MsgReceived msgReceived;
        std::string topic = "topic://test.message.pubsub";
        auto msgBusSender = amqp::MessageBusAmqp("PubTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusSender.connect());

        auto msgBusReceiver = amqp::MessageBusAmqp("PubTestCaseReceiver", AMQP_SERVER_URI);
        REQUIRE(msgBusReceiver.connect());

        REQUIRE(msgBusReceiver.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        Message msg = Message::buildMessage("PubSubTestCase", topic, "TEST", QUERY);
        std::this_thread::sleep_for(TWO_SECONDS);

        for (int i = 0; i < 3; i++) {
            REQUIRE(msgBusSender.send(msg));
            std::this_thread::sleep_for(ONE_SECOND);
            CHECK(msgReceived.isReceived(i + 1));
        }
    }

    SECTION("Unreceive")
    {
        MsgReceived msgReceived;
        auto        msgBus = amqp::MessageBusAmqp("UnreceiveReceiverTestCase", AMQP_SERVER_URI);
        std::string topic  = "topic://test.message.unreceive." + generateUuid();

        // Try to unreceive before a connection => UNAVAILABLE
        REQUIRE(msgBus.unreceive(topic).error() == DeliveryState::Unavailable);
        // After a connection
        REQUIRE(msgBus.connect());
        REQUIRE(msgBus.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        auto msgBusSender = amqp::MessageBusAmqp("UnreceiveSenderTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusSender.connect());

        Message msg = Message::buildMessage("UnreceiveTestCase", topic, "TEST", QUERY);
        REQUIRE(msgBusSender.send(msg));
        std::this_thread::sleep_for(TWO_SECONDS);
        CHECK(msgReceived.isReceived(1));

        // Try to unreceive a wrong topic => REJECTED
        REQUIRE(msgBus.unreceive("/etn/t/wrongTopic").error() == DeliveryState::Rejected);
        // Try to unreceive a right topic => ACCEPTED
        REQUIRE(msgBus.unreceive(topic));
        REQUIRE(msgBusSender.send(msg));
        std::this_thread::sleep_for(ONE_SECOND);
        CHECK(msgReceived.isReceived(1));
    }

    SECTION("Pub sub with same object")
    {
        MsgReceived msgReceived;
        std::string topic = "topic://test.message.sameobject";

        auto msgBus = amqp::MessageBusAmqp("PubTestCaseWithSameObject", AMQP_SERVER_URI);
        REQUIRE(msgBus.connect());

        REQUIRE(msgBus.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        Message msg = Message::buildMessage("PubTestCaseWithSameObject", topic, "TEST", QUERY);
        std::this_thread::sleep_for(TWO_SECONDS);

        REQUIRE(msgBus.send(msg));
        std::this_thread::sleep_for(ONE_SECOND);
        CHECK(msgReceived.isReceived(1));
    }
}

TEST_CASE("wrong", "[amqp][messageStatus]")
{
    SECTION("Wrong message")
    {
        auto msgBus = amqp::MessageBusAmqp("WrongMessageTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBus.connect());
        // Without mandatory fields (from, subject, to)
        auto wrongSendMsg = Message::buildMessage("WrongMessageTestCase", "", "TEST");
        // TODO: Need bus not connected but now send failed in this case
        //REQUIRE(msgBus.send(wrongSendMsg).error() == DeliveryState::Rejected);

        // Without mandatory fields (from, subject, to)
        auto request = Message::buildRequest("WrongRequestTestCase", "", "SyncTest", "", QUERY);
        // Request reject
        REQUIRE(msgBus.request(request, 1).error() == DeliveryState::Rejected);
        request.from("queue://etn.q.request");
        request.to("queue://etn.q.reply");
    }

    SECTION("Wrong ip address")
    {
        auto msgBus        = amqp::MessageBusAmqp("WrongConnectionTestCase", "amqp://wrong.address.ip.com:5672");
        auto connectionRet = msgBus.connect();
        REQUIRE(connectionRet.error() == ComState::ConnectFailed);
    }
}
