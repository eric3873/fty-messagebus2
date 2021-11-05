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
    bool g_received = false;
    auto s_msgBus = fty::messagebus::amqp::MessageBusAmqp("TestCase", AMQP_SERVER_URI);
    auto returnVal2 = s_msgBus.connect();
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
    auto returnVal = s_msgBus.send(response.value());
    if (!returnVal)
    {
      std::cerr << returnVal.error() << std::endl;
    }
  }

  // message listener
  void messageListener(const Message& message)
  {
    std::cout << "messageListener: " << message.toString() << std ::endl;
    g_received = true;
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Amqp identify implementation", "[identity]")
  {
    REQUIRE(s_msgBus.identity() == amqp::BUS_IDENTITY_AMQP);
  }

  TEST_CASE("Mqtt send", "[send]")
  {
    std::string sendTestQueue = "queue://test.message.send";

    // auto msgBus2 = amqp::MessageBusAmqp("AmqpMessageTestCase2", AMQP_SERVER_URI);
    // auto returnVal2 = msgBus2.connect();
    // REQUIRE(returnVal2);

    auto msgBus = amqp::MessageBusAmqp("AmqpMessageTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    // auto returnVal3 = msgBus2.receive(sendTestQueue, messageListener);
    // REQUIRE(returnVal3);

    // Send synchronous request
    Message msg = Message::buildMessage("AmqpMessageTestCase", sendTestQueue, "TEST", QUERY);
    std::cerr << "Message to send:\n" + msg.toString() << std::endl;

   // g_received = false;

    auto returnVal4 = msgBus.send(msg);
    REQUIRE(returnVal4);
    // Wait to process the response
    // std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
    // REQUIRE(g_received);
  }

  TEST_CASE("Amqp request sync", "[request]")
  {
    std::string syncTestQueue = "queue://test.message.sync.";

    auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTestQueue + "request", "SyncTest", syncTestQueue + "reply", QUERY);
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto returnVal2 = s_msgBus.receive(request.to(), replyerAddOK);
    REQUIRE(returnVal2);

    auto replyMsg = msgBus.request(request, MAX_TIMEOUT / 1000);
    REQUIRE(replyMsg);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  }

  TEST_CASE("Amqp request sync timeout reached", "[request]")
  {
    std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.";

    auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

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
    auto msgBus = amqp::MessageBusAmqp("AmqpAsyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    // Send asynchronous request
    Message request = Message::buildRequest("AmqpAsyncRequestTestCase", asyncTestQueue + "request", "TEST", asyncTestQueue + "reply", QUERY);
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto returnVal2 = s_msgBus.receive(request.to(), replyerAddOK);
    REQUIRE(returnVal2);

    g_received = false;
    auto returnVal3 = msgBus.receive(request.replyTo(), messageListener, request.correlationId());
    REQUIRE(returnVal3);

    auto replyMsg = msgBus.send(request);
    REQUIRE(replyMsg);

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));

    REQUIRE(g_received);
  }

  TEST_CASE("Amqp publish receive", "[pub]")
  {
    std::string topic = "topic://test.message.pubsub";
    auto msgBusPub = amqp::MessageBusAmqp("AmqpPubTestCase", AMQP_SERVER_URI);
    auto msgBusSub = amqp::MessageBusAmqp("AmqpSubTestCase", AMQP_SERVER_URI);

    auto returnVal3 = msgBusSub.receive(topic, messageListener);
    REQUIRE(returnVal3);

    Message msg = Message::buildMessage("AmqpPubSubTestCase", topic, "TEST", QUERY);
    g_received = false;

    auto statePub = msgBusPub.send(msg);
    REQUIRE(statePub == DeliveryState::DELIVERY_STATE_ACCEPTED);

    // Wait to process publish
    std::this_thread::sleep_for(std::chrono::milliseconds(MAX_TIMEOUT));
    REQUIRE(g_received);
  }

} // namespace
