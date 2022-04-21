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

#include <catch2/catch.hpp>
#include <future>
#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/messagebus/utils.h>
#include <iostream>
#include <mutex>
#include <thread>

#if defined(EXTERNAL_SERVER_FOR_TEST)
static constexpr auto AMQP_SERVER_URI{"x.x.x.x:5672"};
#else
static constexpr auto AMQP_SERVER_URI{"amqp://127.0.0.1:5672"};
#endif

using namespace fty::messagebus;
using namespace fty::messagebus::utils;

auto constexpr FIVE_HUNDRED_MILLI_SECONDS = std::chrono::milliseconds(500);
auto constexpr SYNC_REQUEST_TIMEOUT       = 2; // in second
auto constexpr MESSAGE_TO_SEND            = 3;

static const std::string QUERY        = "query";
static const std::string OK           = ":OK";
static const std::string QUERY_AND_OK = QUERY + OK;

class MsgReceived
{
private:
    // Mutex
    std::mutex m_lock;
    std::shared_ptr<amqp::MessageBusAmqp> m_msgBusReplyer;

    enum class ExpectedTest
    {
        Receiver            = 0,
        ReceiverAndReplyer  = 1
    };

    bool testAndWaitExpected(const ExpectedTest expectedTest, const int expected)
    {
      int retry = 0;
      bool process = true;
      while (process && (retry <= expected * 2))
      {
        switch (expectedTest)
        {
        case ExpectedTest::Receiver:
          if (receiver == expected)
          {
            process = false;
          }
          break;
        case ExpectedTest::ReceiverAndReplyer:
          if (receiver == expected && replyer == expected)
          {
            process = false;
          }
          break;

        default:
          break;
        }
        if (process)
        {
          std::this_thread::sleep_for(FIVE_HUNDRED_MILLI_SECONDS);
        }
        retry++;
      }
      return !process;
    }

public:
    int receiver;
    int replyer;

    MsgReceived()
        : receiver(0)
        , replyer(0)
    {
      m_msgBusReplyer = std::make_shared<amqp::MessageBusAmqp>("MsgReceivedTestCase", AMQP_SERVER_URI);
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
      return testAndWaitExpected(ExpectedTest::ReceiverAndReplyer, expected);
    }

    bool isRecieved(const int expected)
    {
      return testAndWaitExpected(ExpectedTest::Receiver, expected);
    }

    int getRecieved()
    {
      return receiver;
    }

    void messageListener(const Message&)
    {
      incReceiver();
    }

    void replyerAddOK(const Message& message)
    {
      incReplyer();
      // Build the response
      auto response = message.buildReply(message.userData() + OK);

      if (!response) {
          std::cerr << response.error() << std::endl;
      } else {
        // send the response
        auto msgSent = m_msgBusReplyer->send(response.value());
        if (!msgSent) {
            FAIL(to_string(msgSent.error()));
        }
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

TEST_CASE("requestSync", "[amqp][request]")
{
    SECTION("Send request sync timeout reached")
    {
        MsgReceived msgReceived;
        std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";
        auto        msgBusRequesterSync  = amqp::MessageBusAmqp("SyncRequesterTimeOutTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        auto        msgBusReplyer        = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Send synchronous request
        Message request = Message::
            buildRequest("SyncRequesterTimeOutTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);

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
    }

    SECTION("Send sync request")
    {
      MsgReceived msgReceived;
      std::string syncTestQueue  = "queue://test.message.sync.";
      auto        msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase", AMQP_SERVER_URI);
      REQUIRE(msgBusRequesterSync.connect());

      auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
      REQUIRE(msgBusReplyer.connect());

      // Build synchronous request and set all receiver
      Message request = Message::buildRequest("RequestTestCase", syncTestQueue + "request", "TEST", syncTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);
      REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

      auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
      REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
    }
}

TEST_CASE("requestAsync", "[amqp][request]")
{
    /* SECTION("Send request sync timeout reached")
    {
        MsgReceived msgReceived;
        std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";
        auto        msgBusRequesterSync  = amqp::MessageBusAmqp("SyncRequesterTimeOutTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterSync.connect());

        auto        msgBusReplyer        = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Send synchronous request
        Message request = Message::
            buildRequest("SyncRequesterTimeOutTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);

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
    }

    SECTION("Send sync request")
    {
      MsgReceived msgReceived;
      std::string syncTestQueue  = "queue://test.message.sync.";
      auto        msgBusRequesterSync = amqp::MessageBusAmqp("SyncReceiverTestCase", AMQP_SERVER_URI);
      REQUIRE(msgBusRequesterSync.connect());

      auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
      REQUIRE(msgBusReplyer.connect());

      // Build synchronous request and set all receiver
      Message request = Message::buildRequest("RequestTestCase", syncTestQueue + "request", "TEST", syncTestQueue + "reply", QUERY, {}, SYNC_REQUEST_TIMEOUT);
      REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

      auto replyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
      REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
    } */

    SECTION("Send async request")
    {
        MsgReceived msgReceived;
        std::string asyncTestQueue  = "queue://test.message.async.";
        auto        msgBusRequester = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequester.connect());

        auto msgBusReplyer = amqp::MessageBusAmqp("AsyncReplyerTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusReplyer.connect());

        // Build asynchronous request and set all receiver
        Message request =
            Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
        REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
        REQUIRE(msgBusRequester.receive(
            request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1),
            request.correlationId()));
        int i;
        for (i = 0; i < MESSAGE_TO_SEND; i++) {
            REQUIRE(msgBusReplyer.send(request));
        }
        CHECK(msgReceived.assertValue(i));
    }

        }

TEST_CASE("RequestWithSameObject", "[amqp][request]")
{
    SECTION("Request")
    {
        MsgReceived msgReceived;
        std::string sendTestQueue = "queue://test.message.send";

        auto msgBus = amqp::MessageBusAmqp("MessageRecieverSendTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBus.connect());

        auto msgBusSender = amqp::MessageBusAmqp("MessageSenderSendTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusSender.connect());

        REQUIRE(
            msgBusSender.receive(sendTestQueue, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        // Send message on queue
        Message msg = Message::buildMessage("MqttMessageTestCase", sendTestQueue, "TEST", QUERY);

        int i;
        for (i = 0; i < MESSAGE_TO_SEND; i++) {
            REQUIRE(msgBusSender.send(msg));
        }
        CHECK(msgReceived.isRecieved(i));
    }
}

TEST_CASE("RequestWithSameObject", "[amqp][request]")
{
    SECTION("Request")
    {
      MsgReceived msgReceived;
      std::string asyncTestQueue  = "queue://test.message.sameobject.";

      auto        msgBusRequesterSync = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
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
        auto  msgBusRequesterAsync = amqp::MessageBusAmqp("AsyncRequesterTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusRequesterAsync.connect());

        REQUIRE(msgBusRequesterAsync.receive(
            request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(asyncMsgReceived), std::placeholders::_1),
            request.correlationId()));

          int i = 0;
          for (i = 0; i < MESSAGE_TO_SEND; i++) {
              REQUIRE(msgBusRequesterAsync.send(request));
          }
          CHECK(asyncMsgReceived.isRecieved(i));
          REQUIRE(msgBusRequesterAsync.unreceive(request.replyTo()));
      }

      {
        for (int i = 0; i < MESSAGE_TO_SEND; i++) {
          auto otherReplyMsg = msgBusRequesterSync.request(request, SYNC_REQUEST_TIMEOUT);
          REQUIRE(otherReplyMsg.value().userData() == QUERY_AND_OK);
        }
      }
    }
}

TEST_CASE("topic", "[amqp][pub]")
{
    SECTION("Publish subscribe")
    {
        MsgReceived msgReceived;
        std::string topic        = "topic://test.message.pubsub";
        auto        msgBusSender = amqp::MessageBusAmqp("PubTestCase", AMQP_SERVER_URI);
        REQUIRE(msgBusSender.connect());

        auto msgBusReceiver = amqp::MessageBusAmqp("PubTestCaseReceiver", AMQP_SERVER_URI);
        REQUIRE(msgBusReceiver.connect());

        REQUIRE(msgBusReceiver.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        Message msg = Message::buildMessage("PubSubTestCase", topic, "TEST", QUERY);
        int i;
        for (i = 0; i < MESSAGE_TO_SEND; i++) {
            REQUIRE(msgBusSender.send(msg));
        }
        CHECK(msgReceived.isRecieved(i));
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
        CHECK(msgReceived.isRecieved(1));

        // Try to unreceive a wrong topic => REJECTED
        REQUIRE(msgBus.unreceive("/etn/t/wrongTopic").error() == DeliveryState::Rejected);
        // Try to unreceive a right topic => ACCEPTED
        REQUIRE(msgBus.unreceive(topic));
        REQUIRE(msgBusSender.send(msg));
        CHECK(msgReceived.isRecieved(1));
    }

    SECTION("Pub sub with same object")
    {
        MsgReceived msgReceived;
        std::string topic = "topic://test.message.sameobject";

        auto msgBus = amqp::MessageBusAmqp("PubTestCaseWithSameObject", AMQP_SERVER_URI);
        REQUIRE(msgBus.connect());

        REQUIRE(msgBus.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

        Message msg = Message::buildMessage("PubTestCaseWithSameObject", topic, "TEST", QUERY);

        REQUIRE(msgBus.send(msg));
        CHECK(msgReceived.isRecieved(1));
    }
}

TEST_CASE("wrong", "[amqp][messageStatus]")
{
    SECTION("Wrong message")
    {
        auto msgBus = amqp::MessageBusAmqp("WrongMessageTestCase", AMQP_SERVER_URI);

        // Without mandatory fields (from, subject, to)
        auto wrongSendMsg = Message::buildMessage("WrongMessageTestCase", "", "TEST");
        REQUIRE(msgBus.send(wrongSendMsg).error() == DeliveryState::Rejected);

        // Without mandatory fields (from, subject, to)
        auto request = Message::buildRequest("WrongRequestTestCase", "", "SyncTest", "", QUERY);
        // Request reject
        REQUIRE(msgBus.request(request, 1).error() == DeliveryState::Rejected);
        request.from("queue://etn.q.request");
        request.to("queue://etn.q.reply");
        // Without reply request reject.
        REQUIRE(msgBus.request(request, 1).error() == DeliveryState::Rejected);
    }

    SECTION("Wrong ip address")
    {
        auto msgBus        = amqp::MessageBusAmqp("WrongConnectionTestCase", "amqp://wrong.address.ip.com:5672");
        auto connectionRet = msgBus.connect();
        REQUIRE(connectionRet.error() == ComState::ConnectFailed);
    }
}
