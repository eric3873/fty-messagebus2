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

#include <etn/messagebus/EtnMessage.h>
#include <etn/messagebus/EtnMessageBus.h>
#include <fty/messagebus/MessageBusStatus.h>

#include <catch2/catch.hpp>
#include <iostream>

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

  using namespace etn::messagebus;
  using namespace fty::messagebus;
  using namespace fty::messagebus::utils;

  auto constexpr TIMEOUT = std::chrono::seconds(2);
  static const std::string QUERY = "query";
  static const std::string QUERY_2 = "query2";
  static const std::string OK = ":OK";
  static const std::string QUERY_AND_OK = QUERY + OK;
  static const std::string RESPONSE_2 = QUERY_2 + OK;

  class MsgReceived
  {
  private:
    // Mutex
    std::mutex m_lock;

  public:
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
      std::cout << receiver << " " << replyer << std::endl;
      return (receiver == expected && replyer == expected);
    }

    bool isRecieved(const int expected)
    {
      return (receiver == expected);
    }

    void messageListener(const Message& message)
    {
      incReceiver();
      std::cout << "Message arrived " << message.toString() << std::endl;
    }

    void replyerAddOK(const Message& message)
    {
      incReplyer();
      //Build the response
      auto response = message.buildReply(message.userData() + OK);

      if (!response)
      {
        std::cerr << response.error() << std::endl;
      }

      //send the response
      auto msgBus = amqp::MessageBusAmqp("TestCase", AMQP_SERVER_URI);
      auto connected = msgBus.connect();
      auto msgSent = msgBus.send(response.value());
      if (!msgSent)
      {
        std::cerr << msgSent.error() << std::endl;
      }
    }
  };

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Send", "[etn][request][send]")
  {
    MsgReceived msgReceived{};
    std::string sendTestQueue = buildAddress("test.message.send", AddressType::QUEUE);

    auto msgBus = EtnMessageBus("MessageRecieverSendTestCase", {AMQP_SERVER_URI, MQTT_SERVER_URI});
    auto msgBusSender = EtnMessageBus("MessageSenderSendTestCase");

    REQUIRE(msgBusSender.receive(sendTestQueue, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

    // Send message on queue
    Message msg = Message::buildMessage("MqttMessageTestCase", sendTestQueue, "TEST", QUERY);

    int nbMessageToSend = 3;
    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg));
    }
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(msgReceived.isRecieved(nbMessageToSend));
  }

  TEST_CASE("Send sync request", "[etn][request][sync]")
  {
    MsgReceived msgReceived{};
    std::string syncTestQueue = buildAddress("test.message.sync.", AddressType::REQUEST_QUEUE);
    auto msgBusReciever = EtnMessageBus("SyncReceiverTestCase");

    // Send synchronous request
    Message request = Message::buildRequest("SyncRequesterTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReciever.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));

    // Test without connection before.
    auto msgBusRequester = EtnMessageBus("SyncRequesterTestCase");
    auto replyMsg = msgBusRequester.request(request, 5);
    REQUIRE(replyMsg->userData() == QUERY_AND_OK);
  }

  TEST_CASE("Send async request", "[etn][request][async]")
  {
    MsgReceived msgReceived{};
    ;
    std::string asyncTestQueue = buildAddress("test.message.async.", AddressType::REQUEST_QUEUE);
    auto msgBusRequester = EtnMessageBus("AsyncRequesterTestCase");

    auto msgBusReplyer = EtnMessageBus("AsyncReplyerTestCase");

    // Build asynchronous request and set all receiver
    Message request = Message::buildRequest("AsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReplyer.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1)));
    REQUIRE(msgBusRequester.receive(request.replyTo(), std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1), request.correlationId()));

    for (int i = 0; i < 3; i++)
    {
      REQUIRE(msgBusReplyer.send(request));
      std::this_thread::sleep_for(TIMEOUT);
      CHECK(msgReceived.assertValue(i + 1));
    }
  }

  TEST_CASE("Publish subscribe", "[etn][pub]")
  {
    MsgReceived msgReceived{};
    std::string topic = buildAddress("test.message.pubsub", AddressType::TOPIC);
    auto msgBusSender = EtnMessageBus("PubTestCase");

    auto msgBusReceiver = EtnMessageBus("PubTestCaseReceiver");

    REQUIRE(msgBusReceiver.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

    Message msg = Message::buildMessage("PubSubTestCase", topic, "TEST", QUERY);
    int nbMessageToSend = 3;

    for (int i = 0; i < nbMessageToSend; i++)
    {
      auto delivState = msgBusSender.send(msg);
      REQUIRE(delivState == DeliveryState::DELIVERY_STATE_ACCEPTED);
    }
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(msgReceived.isRecieved(nbMessageToSend));
  }

  TEST_CASE("Publish and subscibe with same object", "[etn][pub]")
  {
    MsgReceived msgReceived{};
    std::string topic = buildAddress("test.message.sameobject", AddressType::TOPIC);

    auto msgBus = EtnMessageBus("PubTestCaseWithSameObject");
    REQUIRE(msgBus.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1)));

    Message msg = Message::buildMessage("PubTestCaseWithSameObject", topic, "TEST", QUERY);
    REQUIRE(msgBus.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    std::this_thread::sleep_for(TIMEOUT);
    CHECK(msgReceived.isRecieved(1));
  }

  TEST_CASE("Wrong broker address", "[etn][request][pub]")
  {
    MsgReceived msgReceived{};
    BrokerAddress brokerAddress{"amqp://wrong.address.ip.com:5672", "tcp://wrong.address.ip.com"};
    auto msgBus = EtnMessageBus("WrongConnectionTestCase", brokerAddress);

    // Topic
    std::string topic = buildAddress("test.message.sameobject", AddressType::TOPIC);
    auto ret = msgBus.receive(topic, std::bind(&MsgReceived::messageListener, std::ref(msgReceived), std::placeholders::_1));
    REQUIRE(ret.error() == to_string(ComState::COM_STATE_CONNECT_FAILED));

    // Request
    std::string syncTestQueue = buildAddress("test.message.sync.", AddressType::REQUEST_QUEUE);
    Message request = Message::buildRequest("SyncRequesterTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);
    ret = msgBus.receive(request.to(), std::bind(&MsgReceived::replyerAddOK, std::ref(msgReceived), std::placeholders::_1));
    REQUIRE(ret.error() == to_string(ComState::COM_STATE_CONNECT_FAILED));
  }
} // namespace
