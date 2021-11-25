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
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>

#include <catch2/catch.hpp>
#include <iostream>

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

  static int MAX_TIMEOUT = 100;
  static const std::string QUERY = "query";
  static const std::string QUERY_2 = "query2";
  static const std::string OK = ":OK";
  static const std::string QUERY_AND_OK = QUERY + OK;
  static const std::string RESPONSE_2 = QUERY_2 + OK;

  namespace
  {
    // struct MsgReceived
    // {
    //   int receiver = 0;
    //   int replyer = 0;

    //   void reset()
    //   {
    //     receiver = 0;
    //     replyer = 0;
    //   }

    //   bool assertValue(const int value)
    //   {
    //     return (receiver == value && replyer == value);
    //   }

    // } g_msgRecieved;

    // void isRecieved(const int expected)
    // {
    //   std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
    //   REQUIRE(g_msgRecieved.receiver == expected);
    // }

    // // Replyer listener
    // void replyerAddOK(const Message& message)
    // {
    //   g_msgRecieved.replyer++;
    //   //Build the response
    //   auto response = message.buildReply(message.userData() + OK);

    //   if (!response)
    //   {
    //     std::cerr << response.error() << std::endl;
    //   }

    //   //send the response
    //   auto msgBus = fty::messagebus::amqp::MessageBusAmqp("TestCase", AMQP_SERVER_URI);
    //   auto connected = msgBus.connect();
    //   auto msgSent = msgBus.send(response.value());
    //   if (!msgSent)
    //   {
    //     std::cerr << msgSent.error() << std::endl;
    //   }
    // }

    // // message listener
    // void messageListener(const Message& message)
    // {
    //   std::cout << "Msg arrived: " << message.toString() << std ::endl;
    //   g_msgRecieved.receiver++;
    // }

    // template <typename T>
    // struct Foo
    // {
    //   size_t size()
    //   {
    //     return 0;
    //   }
    // };


    using MessageBusAmqpPointer = std::shared_ptr<amqp::MessageBusAmqp>;

  } // namespace

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------
  template <typename T>
    struct Foo
    {
//      T(const std::string& clientName, const std::string& endpoint){};

      fty::Expected<void> connect()
      {
        return {};
      }
    };


  TEST_CASE("Test", "[identity]")
  {
    //auto msgBus3 = GENERATE((std::make_shared<amqp::MessageBusAmqp>("AmqpMessageIdentityTestCase")), (std::make_shared<mqtt::MessageBusMqtt>("AmqpMessageIdentityTestCase")));
    auto msgBus3 = GENERATE(std::make_shared<mqtt::MessageBusMqtt>("AmqpMessageIdentityTestCase"), std::make_shared<mqtt::MessageBusMqtt>("AmqpMessageIdentityTestCase"));
    // auto i = GENERATE(1, 3, 5);
    // auto msgBus = amqp::MessageBusAmqp("AmqpMessageIdentityTestCase", AMQP_SERVER_URI);
    // REQUIRE(msgBus.identity() == amqp::BUS_IDENTITY_AMQP);
    //REQUIRE(msgBus3->identity() == amqp::BUS_IDENTITY_AMQP);
    REQUIRE(msgBus3->clientName() == "AmqpMessageIdentityTestCase");

    // auto msgBus2 = mqtt::MessageBusMqtt("MqttMessageTestCase", MQTT_SERVER_URI);
    REQUIRE(msgBus3->identity() == mqtt::BUS_IDENTITY);
    // REQUIRE(msgBus2.clientName() == "MqttMessageTestCase");
  }

  TEMPLATE_PRODUCT_TEST_CASE("Test identity", "[identity]", (amqp::MessageBusAmqp, Foo), ("AmqpMessageIdentityTestCase", ""))
  {
    TestType x;
    // REQUIRE(x.clientName() == "");
    std::cout << "TOTO" << std::endl;
  }

  // TEMPLATE_PRODUCT_TEST_CASE("Test identity", "[identity]", (std::vector, Foo), (int))
  // {
  //   TestType x;
  //   REQUIRE(x.size() == 0);
  //   std::cout << "TOTO" << std::endl;
  // }

  // TEMPLATE_PRODUCT_TEST_CASE("Test identity", "[identity]", (mqtt::MessageBusMqtt, Foo), (int))
  // {
  //   TestType x;
  //   REQUIRE(msgBus3->clientName() == "AmqpMessageIdentityTestCase");
  //   std::cout << "TOTO" << std::endl;
  // }

  // TEST_CASE("Amqp send", "[send]")
  // {
  //   std::string sendTestQueue = "queue://test.message.send";

  //   auto msgBusReciever = amqp::MessageBusAmqp("AmqpMessageRecieverSendTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusReciever.connect());

  //   auto msgBusSender = amqp::MessageBusAmqp("AmqpMessageSenderSendTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusSender.connect());

  //   REQUIRE(msgBusReciever.receive(sendTestQueue, messageListener));

  //   // Send synchronous request
  //   Message msg = Message::buildMessage("AmqpMessageTestCase", sendTestQueue, "TEST", QUERY);

  //   g_msgRecieved.reset();
  //   int nbMessageToSend = 3;
  //   for (int i = 0; i < nbMessageToSend; i++)
  //   {
  //     REQUIRE(msgBusSender.send(msg));
  //   }

  //   isRecieved(nbMessageToSend);
  // }

  // TEST_CASE("Amqp request sync", "[request]")
  // {
  //   std::string syncTestQueue = "queue://test.message.sync.";
  //   // Send synchronous request
  //   Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);

  //   auto msgBusReciever = amqp::MessageBusAmqp("AmqpSyncReceiverTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusReciever.connect());
  //   REQUIRE(msgBusReciever.receive(request.to(), replyerAddOK));

  //   // Test without connection before.
  //   auto msgBusRequester = amqp::MessageBusAmqp("AmqpSyncRequesterTestCase", AMQP_SERVER_URI);
  //   auto requester = msgBusRequester.request(request, MAX_TIMEOUT / 100);
  //   REQUIRE(requester.error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));

  //   // Test with connection after.
  //   REQUIRE(msgBusRequester.connect());
  //   auto replyMsg = msgBusRequester.request(request, MAX_TIMEOUT / 100);
  //   REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  // }

  // TEST_CASE("Amqp request sync timeout reached", "[request]")
  // {
  //   std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";

  //   auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequesterTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBus.connect());

  //   // Send synchronous request
  //   Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", "test:");

  //   auto replyMsg = msgBus.request(request, 1);
  //   REQUIRE(!replyMsg);
  //   REQUIRE(from_deliveryState(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  // }

  // TEST_CASE("Amqp async request", "[request]")
  // {
  //   std::string asyncTestQueue = "queue://test.message.async.";
  //   auto msgBusRequester = amqp::MessageBusAmqp("AmqpAsyncRequesterTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusRequester.connect());

  //   auto msgBusReplyer = amqp::MessageBusAmqp("AmqpAsyncReplyerTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusReplyer.connect());

  //   // Build asynchronous request and set all receiver
  //   Message request = Message::buildRequest("AmqpAsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
  //   REQUIRE(msgBusReplyer.receive(request.to(), replyerAddOK));
  //   REQUIRE(msgBusRequester.receive(request.replyTo(), messageListener, request.correlationId()));

  //   g_msgRecieved.reset();
  //   for (int i = 0; i < 2; i++)
  //   {
  //     REQUIRE(msgBusRequester.send(request));
  //     std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
  //     REQUIRE((g_msgRecieved.assertValue(i + 1)));
  //   }
  // }

  // TEST_CASE("Amqp publish receive", "[pubSub]")
  // {
  //   std::string topic = "topic://test.message.pubsub";

  //   auto msgBusSender = amqp::MessageBusAmqp("AmqpPubTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusSender.connect());

  //   auto msgBusReceiver = amqp::MessageBusAmqp("AmqpSubTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusReceiver.connect());

  //   REQUIRE(msgBusReceiver.receive(topic, messageListener));

  //   Message msg = Message::buildMessage("AmqpPubSubTestCase", topic, "TEST", QUERY);
  //   g_msgRecieved.reset();
  //   int nbMessageToSend = 3;

  //   for (int i = 0; i < nbMessageToSend; i++)
  //   {
  //     REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
  //   }

  //   isRecieved(nbMessageToSend);
  // }

  // TEST_CASE("Amqp unreceive", "[pubSub]")
  // {
  //   std::string topic = "topic://test.message.unreceive";

  //   auto msgBusReceiver = amqp::MessageBusAmqp("AmqpUnreceiveReceiverTestCase", AMQP_SERVER_URI);
  //   // Try to unreceive before a connection => UNAVAILABLE
  //   REQUIRE(msgBusReceiver.unreceive(topic).error() == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE));
  //   REQUIRE(msgBusReceiver.connect());
  //   REQUIRE(msgBusReceiver.receive(topic, messageListener));

  //   auto msgBusSender = amqp::MessageBusAmqp("AmqpUnreceiveSenderTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBusSender.connect());

  //   Message msg = Message::buildMessage("AmqpUnreceiveTestCase", topic, "TEST", QUERY);
  //   g_msgRecieved.reset();
  //   REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
  //   isRecieved(1);

  //   // Try to unreceive a wrong topic => REJECTED
  //   REQUIRE(msgBusReceiver.unreceive("topic://wrong.topic").error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
  //   // Try to unreceive a wrong topic => ACCEPTED
  //   REQUIRE(msgBusReceiver.unreceive(topic));
  //   REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
  //   isRecieved(1);
  // }

  // TEST_CASE("Amqp pub sub with same object", "[pubsub]")
  // {
  //   std::string topic = "topic://test.message.sameobject";

  //   auto msgBus = amqp::MessageBusAmqp("AmqpUnreceiveSenderTestCase", AMQP_SERVER_URI);
  //   REQUIRE(msgBus.connect());

  //   REQUIRE(msgBus.receive(topic, messageListener));

  //   Message msg = Message::buildMessage("AmqpUnreceiveTestCase", topic, "TEST", QUERY);
  //   g_msgRecieved.reset();
  //   REQUIRE(msgBus.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
  //   isRecieved(1);
  // }

  // TEST_CASE("Amqp wrong connection", "[messageStatus]")
  // {
  //   auto msgBus = amqp::MessageBusAmqp("AmqpMessageBusStatusTestCase", "amqp://wrong.address.ip.com:5672");
  //   auto connectionRet = msgBus.connect();
  //   REQUIRE(connectionRet.error() == to_string(ComState::COM_STATE_CONNECT_FAILED));
  // }

  // TEST_CASE("Amqp wrong message", "[messageStatus]")
  // {
  //   auto msgBus = amqp::MessageBusAmqp("AmqpNoConnectionTestCase", AMQP_SERVER_URI);

  //   // Without mandatory fields (from, subject, to)
  //   auto wrongSendMsg = Message::buildMessage("AmqpNoConnectionTestCase", "", "TEST");
  //   auto sent = msgBus.send(wrongSendMsg);
  //   REQUIRE(sent.error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));

  //   // Without mandatory fields (from, subject, to)
  //   auto request = Message::buildRequest("AmqpSyncRequestTestCase", "", "SyncTest", "", QUERY);
  //   // Request reject
  //   REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
  //   request.from("queue://request");
  //   request.to("queue://reply");
  //   // Without reply request reject.
  //   REQUIRE(msgBus.request(request, 1).error() == to_string(DeliveryState::DELIVERY_STATE_REJECTED));
  // }

} // namespace
