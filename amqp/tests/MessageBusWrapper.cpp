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

#include "MessageBusWrapper.h"
#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>

#include <catch2/catch.hpp>
#include <cstdlib>
#include <cxxabi.h>
#include <iostream>
#include <regex>

#include <mutex>
#include <thread>

namespace
{
#if defined(EXTERNAL_SERVER_FOR_TEST)
  static constexpr auto AMQP_SERVER_URI{"x.x.x.x:5672"};
  static constexpr auto MQTT_SERVER_URI{"tcp://mqtt.eclipse.org:1883"};
#else
  static constexpr auto AMQP_SERVER_URI{"amqp://127.0.0.1:5672"};
  static constexpr auto MQTT_SERVER_URI{"tcp://localhost:1883"};
#endif

  using namespace fty::messagebus;

  namespace
  {
    static int MAX_TIMEOUT = 100;
    static const std::string QUERY = "query";
    static const std::string QUERY_2 = "query2";
    static const std::string OK = ":OK";
    static const std::string QUERY_AND_OK = QUERY + OK;
    static const std::string RESPONSE_2 = QUERY_2 + OK;

    // Mutex
    std::mutex m_lock;

    struct MsgReceived
    {
      int receiver;
      int replyer;

      MsgReceived()
        : receiver(0)
        , replyer(0)
      {
      }

      void reset()
      {
        std::lock_guard<std::mutex> lock(m_lock);
        receiver = 0;
        replyer = 0;
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

      bool assertValue(const int value)
      {
        std::lock_guard<std::mutex> lock(m_lock);
        return (receiver == value && replyer == value);
      }
    };

    auto g_msgRecieved = MsgReceived();

    void isRecieved(const int expected)
    {
      std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
      REQUIRE(g_msgRecieved.receiver == expected);
    }

    // Replyer listener
    void replyerAddOK(const Message& message)
    {
      g_msgRecieved.incReplyer();
      //Build the response
      auto response = message.buildReply(message.userData() + OK);

      if (!response)
      {
        std::cerr << response.error() << std::endl;
      }

      //send the response
      auto msgBus = fty::messagebus::amqp::MessageBusAmqp("TestCase", AMQP_SERVER_URI);
      auto connected = msgBus.connect();
      auto msgSent = msgBus.send(response.value());
      if (!msgSent)
      {
        std::cerr << msgSent.error() << std::endl;
      }
    }

    // message listener
    void messageListener(const Message& message)
    {
      std::cout << "Msg arrived: " << message.toString() << std ::endl;
      g_msgRecieved.incReceiver();
    }

  } // namespace

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Identify", "[amqp][mqtt][identity]", amqp::MessageBusAmqp, mqtt::MessageBusMqtt)
  {
    auto context = MsgBusFixture<TestType>("AmqpUnreceiveReceiverTestCase");

    REQUIRE(context.m_bus.clientName() == "AmqpUnreceiveReceiverTestCase");
    REQUIRE(context.m_bus.identity() == context.getIdentity());
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Send", "[amqp][mqtt][send]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("AmqpMessageRecieverSendTestCase");
    std::string sendTestQueue = context.getAddress("/etn.test.message.send");

    REQUIRE(context.m_bus.connect());

    auto msgBusSender = MsgBusFixture<TestType>("AmqpMessageSenderSendTestCase").m_bus;
    REQUIRE(msgBusSender.connect());

    REQUIRE(context.m_bus.receive(sendTestQueue, messageListener));

    // Send synchronous request
    Message msg = Message::buildMessage("AmqpMessageTestCase", sendTestQueue, "TEST", QUERY);

    g_msgRecieved.reset();
    int nbMessageToSend = 3;
    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg));
    }
    isRecieved(nbMessageToSend);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Send request sync", "[amqp][mqtt][request][sync]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("SyncReceiverTestCase");
    std::string syncTestQueue = context.getAddress("/etn.test.message.sync.");

    // Send synchronous request
    Message request = Message::buildRequest("SyncRequesterTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    REQUIRE(context.m_bus.connect());
    REQUIRE(context.m_bus.receive(request.to(), replyerAddOK));
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // Test without connection before.
    auto msgBusRequester = MsgBusFixture<TestType>("AmqpSyncRequesterTestCase").m_bus;
    auto requester = msgBusRequester.request(request, 5);
    REQUIRE(requester.error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));

    // Test with connection after.
    REQUIRE(msgBusRequester.connect());
    auto replyMsg = msgBusRequester.request(request, MAX_TIMEOUT / 100);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Send request sync timeout reached", "[amqp][mqtt][request][sync]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("SyncRequesterTestCase");
    std::string syncTimeOutTestQueue = context.getAddress("/etn.test.message.synctimeout.");

    REQUIRE(context.m_bus.connect());

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", "test:");

    auto replyMsg = context.m_bus.request(request, 2);
    REQUIRE(!replyMsg);
    REQUIRE(from_deliveryState(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Send async request", "[amqp][mqtt][request][async]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("AsyncRequesterTestCase");
    std::string asyncTestQueue = context.getAddress("/etn.test.message.async.");
    REQUIRE(context.m_bus.connect());

    auto msgBusReplyer = MsgBusFixture<TestType>("AmqpAsyncReplyerTestCase").m_bus;
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request = Message::buildRequest("AmqpAsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReplyer.receive(request.to(), replyerAddOK));
    REQUIRE(context.m_bus.receive(request.replyTo(), messageListener, request.correlationId()));

    g_msgRecieved.reset();
    for (int i = 0; i < 2; i++)
    {
      REQUIRE(context.m_bus.send(request));
      std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
      REQUIRE((g_msgRecieved.assertValue(i + 1)));
    }
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Publish subscribe", "[amqp][mqtt][pub]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("PubTestCase");
    std::string topic = context.getAddress("/etn.test.message.pubsub", TOPIC);
    REQUIRE(context.m_bus.connect());

    auto msgBusReceiver = MsgBusFixture<TestType>("AmqpSubTestCase").m_bus;
    REQUIRE(msgBusReceiver.connect());

    REQUIRE(msgBusReceiver.receive(topic, messageListener));

    Message msg = Message::buildMessage("AmqpPubSubTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    int nbMessageToSend = 3;

    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(context.m_bus.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    }

    isRecieved(nbMessageToSend);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Unreceive", "[amqp][mqtt][pub]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("UnreceiveReceiverTestCase");
    std::string topic = context.getAddress("/etn.test.message.unreceive", TOPIC);

    // Try to unreceive before a connection => UNAVAILABLE
    REQUIRE(context.m_bus.unreceive(topic).error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    REQUIRE(context.m_bus.connect());
    REQUIRE(context.m_bus.receive(topic, messageListener));

    auto msgBusSender = MsgBusFixture<TestType>("AmqpUnreceiveSenderTestCase").m_bus;
    REQUIRE(msgBusSender.connect());

    Message msg = Message::buildMessage("AmqpUnreceiveTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    isRecieved(1);

    // Try to unreceive a wrong topic => REJECTED
    REQUIRE(context.m_bus.unreceive("topic://wrong.topic").error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    // Try to unreceive a wrong topic => ACCEPTED
    REQUIRE(context.m_bus.unreceive(topic));
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    isRecieved(1);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Pub sub with same object", "[amqp][mqtt][pub]", amqp::MessageBusAmqp /*, mqtt::MessageBusMqtt*/)
  {
    auto context = MsgBusFixture<TestType>("AmqpUnreceiveSenderTestCase");
    std::string topic = context.getAddress("/etn.test.message.sameobject", TOPIC);

    auto msgBus = amqp::MessageBusAmqp("AmqpUnreceiveSenderTestCase", AMQP_SERVER_URI);
    REQUIRE(context.m_bus.connect());

    REQUIRE(context.m_bus.receive(topic, messageListener));

    Message msg = Message::buildMessage("AmqpUnreceiveTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    REQUIRE(context.m_bus.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    isRecieved(1);
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Wrong connection", "[amqp][mqtt][messageStatus]", amqp::MessageBusAmqp/*, mqtt::MessageBusMqtt*/)
  {
    auto msgBus = MsgBusFixture<TestType>("AmqpUnreceiveSenderTestCase", "tcp://wrong.address.ip.com:5672").m_bus;
    REQUIRE(msgBus.connect().error() == to_string(ComState::COM_STATE_CONNECT_FAILED));
  }

  TEMPLATE_TEST_CASE_METHOD(MsgBusFixture, "Wrong message", "[amqp][mqtt][messageStatus]", amqp::MessageBusAmqp, mqtt::MessageBusMqtt)
  {
    auto msgBus = MsgBusFixture<TestType>("AmqpNoConnectionTestCase").m_bus;

    // Without mandatory fields (from, subject, to)
    auto wrongSendMsg = Message::buildMessage("AmqpNoConnectionTestCase", "", "TEST");
    REQUIRE(msgBus.send(wrongSendMsg).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));

    // Without mandatory fields (from, subject, to)
    auto request = Message::buildRequest("AmqpSyncRequestTestCase", "", "SyncTest", "", QUERY);
    // Request reject
    REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    request.from("queue://request");
    request.to("queue://reply");
    // Without reply request reject.
    REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
  }

} // namespace
