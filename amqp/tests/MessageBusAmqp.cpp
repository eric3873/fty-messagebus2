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

#include <catch2/catch.hpp>
#include <iostream>

#include <thread>

namespace
{
#if defined(EXTERNAL_SERVER_FOR_TEST)
  static constexpr auto AMQP_SERVER_URI{"x.x.x.x:5672"};
#else
  static constexpr auto AMQP_SERVER_URI{"amqp://127.0.0.1:5672"};
#endif

  using namespace fty::messagebus;

  static int MAX_TIMEOUT = 1000;
  static const std::string QUERY = "query";
  static const std::string QUERY_2 = "query2";
  static const std::string OK = ":OK";
  static const std::string QUERY_AND_OK = QUERY + OK;
  static const std::string RESPONSE_2 = QUERY_2 + OK;

  namespace
  {
    int g_nbMsgReceived = 0;
  } // namespace

  // Replyer listener
  void replyerAddOK(const Message& message)
  {
    std::cout << "replyerListener: " << message.toString() << std ::endl;
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
    g_nbMsgReceived++;
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Amqp identify implementation", "[identity]")
  {
    auto msgBus = amqp::MessageBusAmqp("AmqpMessageIdentityTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBus.identity() == amqp::BUS_IDENTITY_AMQP);
  }

  TEST_CASE("Amqp send", "[send]")
  {
    std::string sendTestQueue = "queue://test.message.send";

    auto msgBusReciever = amqp::MessageBusAmqp("AmqpMessageRecieverSendTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReciever.connect());

    auto msgBusSender = amqp::MessageBusAmqp("AmqpMessageSenderSendTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    REQUIRE(msgBusReciever.receive(sendTestQueue, messageListener));

    // Send synchronous request
    Message msg = Message::buildMessage("AmqpMessageTestCase", sendTestQueue, "TEST", QUERY);
    std::cerr << "Message to send:\n" + msg.toString() << std::endl;

    g_nbMsgReceived = 0;
    int nbMessageToSend = 3;
    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg));
    }

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
    REQUIRE(g_nbMsgReceived == nbMessageToSend);
  }

  TEST_CASE("Amqp request sync", "[request]")
  {
    std::string syncTestQueue = "queue://test.message.sync.";

    auto msgBusRequester = amqp::MessageBusAmqp("AmqpSyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester.connect());

    auto msgBusReciever = amqp::MessageBusAmqp("AmqpSyncReceiverTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReciever.connect());

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);

    REQUIRE(msgBusReciever.receive(request.to(), replyerAddOK));

    auto replyMsg = msgBusRequester.request(request, MAX_TIMEOUT / 100);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  }

  TEST_CASE("Amqp request sync timeout reached", "[request]")
  {
    std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";

    auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBus.connect());

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTimeOutTestQueue + "request", "TEST", syncTimeOutTestQueue + "reply", "test:");
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.request(request, 2);
    REQUIRE(!replyMsg);
    REQUIRE(from_deliveryState(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  }

  TEST_CASE("Amqp async request", "[request]")
  {
    std::string asyncTestQueue = "queue://test.message.async.";
    auto msgBusRequester = amqp::MessageBusAmqp("AmqpAsyncRequesterTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusRequester.connect());

    auto msgBusReciever = amqp::MessageBusAmqp("AmqpAsyncReplyerTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReciever.connect());

    // Send asynchronous request
    Message request = Message::buildRequest("AmqpAsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    REQUIRE(msgBusReciever.receive(request.to(), replyerAddOK));

    g_nbMsgReceived = 0;
    REQUIRE(msgBusRequester.receive(request.replyTo(), messageListener, request.correlationId()));

    REQUIRE(msgBusRequester.send(request));

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));

    REQUIRE(g_nbMsgReceived == 1);
  }

  TEST_CASE("Amqp publish receive", "[pubSub]")
  {
    std::string topic = "topic://test.message.pubsub";

    auto msgBusSender = amqp::MessageBusAmqp("AmqpPubTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    auto msgBusReceiver = amqp::MessageBusAmqp("AmqpSubTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReceiver.connect());

    REQUIRE(msgBusReceiver.receive(topic, messageListener));

    Message msg = Message::buildMessage("AmqpPubSubTestCase", topic, "TEST", QUERY);
    g_nbMsgReceived = 0;
    int nbMessageToSend = 3;

    for (int i = 0; i < nbMessageToSend; i++)
    {
      REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    }

    // // Wait to process publish
    std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
    REQUIRE(g_nbMsgReceived == nbMessageToSend);
  }

  TEST_CASE("Amqp unreceive", "[sub]")
  {
    std::string topic = "topic://test.message.unreceive";

    auto msgBusSender = amqp::MessageBusAmqp("AmqpUnreceiveSenderTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusSender.connect());

    auto msgBusReceiver = amqp::MessageBusAmqp("AmqpUnreceiveReceiverTestCase", AMQP_SERVER_URI);
    REQUIRE(msgBusReceiver.connect());

    REQUIRE(msgBusReceiver.receive(topic, messageListener));

    Message msg = Message::buildMessage("AmqpUnreceiveTestCase", topic, "TEST", QUERY);
    g_nbMsgReceived = 0;
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    REQUIRE(g_nbMsgReceived == 1);

    REQUIRE(msgBusReceiver.unreceive(topic));
    REQUIRE(msgBusSender.send(msg) == DeliveryState::DELIVERY_STATE_ACCEPTED);
    REQUIRE(g_nbMsgReceived == 1);
  }

} // namespace
