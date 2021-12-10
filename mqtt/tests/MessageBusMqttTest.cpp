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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>

#include <catch2/catch.hpp>
#include <iostream>

#include <mutex>
#include <thread>

namespace
{
  // NOTE: This test case requires network access. It uses one of
  // the public available MQTT brokers
#if defined(EXTERNAL_SERVER_FOR_TEST)
  static constexpr auto MQTT_SERVER_URI{"tcp://mqtt.eclipse.org:1883"};
#else
  static constexpr auto MQTT_SERVER_URI{"tcp://localhost:1883"};
#endif

  using namespace fty::messagebus;

  auto constexpr TIMEOUT = std::chrono::seconds(2);
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

    bool assertValue(const int expected)
    {
      return (receiver == expected && replyer == expected);
    }

    bool isRecieved(const int expected)
    {
      return (receiver == expected);
    }
  };

  auto g_msgRecieved = MsgReceived();

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
    auto msgBus = mqtt::MessageBusMqtt("TestCase", MQTT_SERVER_URI);
    auto connected = msgBus.connect();
    auto msgSent = msgBus.send(response.value());
    if (!msgSent)
    {
      std::cerr << msgSent.error() << std::endl;
    }
  }

  // message listener
  void messageListener(const Message& /*message*/)
  {
    g_msgRecieved.incReceiver();
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Identity", "[identity]")
  {
    auto msgBus = mqtt::MessageBusMqtt("IdentitytestCase", MQTT_SERVER_URI);
    REQUIRE(msgBus.clientName() == "IdentitytestCase");
    REQUIRE(msgBus.identity() == mqtt::BUS_IDENTITY);
  }

  TEST_CASE("Send", "[mqtt][request][send]")
  {
    std::string sendTestQueue = "/test/message/send";
    auto msgBus = mqtt::MessageBusMqtt("MessageRecieverSendTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBus.connect());

    auto msgBusSender = mqtt::MessageBusMqtt("MessageSenderSendTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    REQUIRE(msgBusSender.receive(sendTestQueue, messageListener));

    // Send synchronous request
    Message msg = Message::buildMessage("MqttMessageTestCase", sendTestQueue, "TEST", QUERY);

    g_msgRecieved.reset();
    int nbMessageToSend = 3;
    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg));
    }
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(g_msgRecieved.isRecieved(nbMessageToSend));
  }

  TEST_CASE("Send sync request", "[mqtt][request][sync]")
  {
    std::string syncTestQueue = "/etn/test/message/sync/";

    auto msgBusReciever = mqtt::MessageBusMqtt("SyncReceiverTestCase", MQTT_SERVER_URI);

    // Send synchronous request
    Message request = Message::buildRequest("SyncRequesterTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);

    REQUIRE(msgBusReciever.connect());
    REQUIRE(msgBusReciever.receive(request.to(), replyerAddOK));

    // Test without connection before.
    auto msgBusRequester = mqtt::MessageBusMqtt("SyncRequesterTestCase", MQTT_SERVER_URI);
    auto requester = msgBusRequester.request(request, 5);
    REQUIRE(requester.error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));

    // Test with connection after.
    REQUIRE(msgBusRequester.connect());
    auto replyMsg = msgBusRequester.request(request, 5);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  }

  TEST_CASE("Send request sync timeout reached", "[mqtt][request][sync]")
  {
    std::string syncTimeOutTestQueue = "/etn/test/message/synctimeout/";
    auto msgBus = mqtt::MessageBusMqtt("SyncRequesterTimeOutTestCase", MQTT_SERVER_URI);

    REQUIRE(msgBus.connect());

    // Send synchronous request
    Message request = Message::buildRequest("SyncRequesterTimeOutTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", "test:");

    auto replyMsg = msgBus.request(request, 1);
    REQUIRE(!replyMsg);
    REQUIRE(from_deliveryState(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  }

  TEST_CASE("Send async request", "[mqtt][request][async]")
  {
    std::string asyncTestQueue = "/etn/test/message/async/";
    auto msgBusRequester = mqtt::MessageBusMqtt("AsyncRequesterTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBusRequester.connect());

    auto msgBusReplyer = mqtt::MessageBusMqtt("AsyncReplyerTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBusReplyer.connect());

    // Build asynchronous request and set all receiver
    Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReplyer.receive(request.to(), replyerAddOK));
    REQUIRE(msgBusRequester.receive(request.replyTo(), messageListener, request.correlationId()));

    g_msgRecieved.reset();
    for (int i = 0; i < 2; i++)
    {
      REQUIRE(msgBusReplyer.send(request));
      std::this_thread::sleep_for(TIMEOUT);
      REQUIRE((g_msgRecieved.assertValue(i + 1)));
    }
  }

  TEST_CASE("Publish subscribe", "[mqtt][pub]")
  {
    auto topic = "/etn/test/message/pubsub";
    auto msgBusSender = mqtt::MessageBusMqtt("PubTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    auto msgBusReceiver = mqtt::MessageBusMqtt("PubTestCaseReceiver", MQTT_SERVER_URI);
    REQUIRE(msgBusReceiver.connect());

    REQUIRE(msgBusReceiver.receive(topic, messageListener));

    Message msg = Message::buildMessage("PubSubTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    int nbMessageToSend = 3;

    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    }
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(g_msgRecieved.isRecieved(nbMessageToSend));
  }

  TEST_CASE("Unreceive", "[mqtt][pub]")
  {
    auto msgBus = mqtt::MessageBusMqtt("UnreceiveReceiverTestCase", MQTT_SERVER_URI);
    std::string topic = "/etn/test/message/unreceive";

    // Try to unreceive before a connection => UNAVAILABLE
    REQUIRE(msgBus.unreceive(topic).error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
    REQUIRE(msgBus.connect());
    REQUIRE(msgBus.receive(topic, messageListener));

    auto msgBusSender = mqtt::MessageBusMqtt("UnreceiveSenderTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    Message msg = Message::buildMessage("UnreceiveTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(g_msgRecieved.isRecieved(1));

    // Try to unreceive a wrong topic => REJECTED
    REQUIRE(msgBus.unreceive("/etn/t/wrongTopic").error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    // Try to unreceive a right topic => ACCEPTED
    REQUIRE(msgBus.unreceive(topic));
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(g_msgRecieved.isRecieved(1));
  }

  TEST_CASE("Pub sub with same object", "[mqtt][pub]")
  {
    auto topic = "/etn/test/message/pubsub";

    auto msgBus = mqtt::MessageBusMqtt("PubTestCaseWithSameObject", MQTT_SERVER_URI);
    REQUIRE(msgBus.connect());

    REQUIRE(msgBus.receive(topic, messageListener));

    Message msg = Message::buildMessage("UnreceiveTestCase", topic, "TEST", QUERY);
    g_msgRecieved.reset();
    REQUIRE(msgBus.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(g_msgRecieved.isRecieved(1));
  }

  TEST_CASE("Wrong message", "[mqtt][messageStatus]")
  {
    auto msgBus = mqtt::MessageBusMqtt("WrongMessageTestCase", MQTT_SERVER_URI);

    // Without mandatory fields (from, subject, to)
    auto wrongSendMsg = Message::buildMessage("WrongMessageTestCase", "", "TEST");
    REQUIRE(msgBus.send(wrongSendMsg).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));

    // Without mandatory fields (from, subject, to)
    auto request = Message::buildRequest("WrongRequestTestCase", "", "SyncTest", "", QUERY);
    // Request reject
    REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
    request.from("/etn/q/request");
    request.to("/etn/q/reply");
    // Without reply request reject.
    REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
  }

  TEST_CASE("Wrong connection", "[mqtt][commStateStatus]")
  {
    auto msgBus = mqtt::MessageBusMqtt("WrongConnectionTestCase", "tcp://wrong.address.ip.com");
    auto connectionRet = msgBus.connect();
    REQUIRE(connectionRet.error() == to_string(ComState::COM_STATE_CONNECT_FAILED));
  }

} // namespace
