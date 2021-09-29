/*  =========================================================================
    MessageBusAmqp.cpp - description

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

  static int MAX_TIMEOUT = 1;
  static const std::string QUERY = "query";
  static const std::string QUERY_2 = "query2";
  static const std::string OK = ":OK";
  static const std::string QUERY_AND_OK = QUERY + OK;
  static const std::string RESPONSE_2 = QUERY_2 + OK;

  static constexpr auto TEST_TOPIC = "examples";
  static constexpr auto TEST_QUEUE = "examples";

  namespace
  {
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

  void replyerTimeout(const Message& message)
  {
    std::cerr << "Message in replyerTimeout:\n" + message.toString() << std::endl;
    //Build the response
    auto response = message.buildReply(message.userData() + OK);

    if (!response)
    {
      std::cerr << response.error() << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT + 1));

    //send the response
    auto returnVal = s_msgBus.send(response.value());

    if (!returnVal)
    {
      std::cerr << returnVal.error() << std::endl;
    }
  }

  bool g_received = false;
  // message listener
  void messageListener(Message message)
  {
    std::cout << "responseListener: " << message.userData() << std ::endl;
    g_received = true;
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Amqp identify implementation", "[identity]")
  {
    REQUIRE(s_msgBus.identity() == amqp::BUS_INDENTITY_AMQP);
  }

  TEST_CASE("Mqtt send", "[send]")
  {
    std::string sendTestQueue = "queue://test.message.send";

    auto msgBus = amqp::MessageBusAmqp("AmqpMessageTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto msgBus2 = amqp::MessageBusAmqp("AmqpMessageTestCase2", AMQP_SERVER_URI);
    auto returnVal2 = msgBus2.connect();
    REQUIRE(returnVal2);

    auto returnVal3 = msgBus2.receive(sendTestQueue, messageListener);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message msg = Message::buildMessage("AmqpMessageTestCase", sendTestQueue, "TEST", QUERY);
    std::cerr << "Message to send:\n" + msg.toString() << std::endl;

    g_received = false;

    auto returnVal4 = msgBus.send(msg);
    REQUIRE(returnVal4);
    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT));

    REQUIRE(g_received);
  }

  TEST_CASE("Amqp request sync", "[request]")
  {
    std::string syncTestQueue = "queue://test.message.sync.request";

    auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto returnVal3 = msgBus.receive(syncTestQueue, replyerAddOK);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTestQueue, "SyncTest", syncTestQueue + "/reply", QUERY);
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.request(request, MAX_TIMEOUT);
    REQUIRE(replyMsg);
    REQUIRE(replyMsg.value().userData() == QUERY_AND_OK);
  }

  TEST_CASE("Amqp request sync timeout reached", "[request]")
  {
    std::string syncTimeOutTestQueue = "queue://test.message.synctimeout.request";

    auto msgBus = amqp::MessageBusAmqp("AmqpSyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    // Send synchronous request
    Message request = Message::buildRequest("AmqpSyncRequestTestCase", syncTimeOutTestQueue, "TEST", syncTimeOutTestQueue + "/reply", "test:");
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.request(request, MAX_TIMEOUT);
    REQUIRE(!replyMsg);
    REQUIRE(from_deliveryState_state(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  }

  TEST_CASE("Amqp async request", "[send]")
  {
    std::string asyncTestQueue = "queue://test.message.async.request";
    auto msgBus = amqp::MessageBusAmqp("AmqpAsyncRequestTestCase", AMQP_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto returnVal3 = s_msgBus.receive(asyncTestQueue, replyerAddOK);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message request = Message::buildRequest("AmqpAsyncRequestTestCase", asyncTestQueue, "TEST", asyncTestQueue + "/reply", QUERY);
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.send(request);
    REQUIRE(replyMsg);

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT));
  }

  // TEST_CASE("Amqp publish receive", "[hide]")
  // {
  //   auto msgBus = MsgBusAmqp("AmqpPubSubTestCase", AMQP_SERVER_URI);
  //   DeliveryState state;

  //   // TODO see only for subscribing
  //   state = msgBus.receive(TEST_TOPIC, {});
  //   REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);

  //   state = msgBus.publish(TEST_TOPIC, RESPONSE);
  //   REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);
  //   // Wait to process publish
  //   std::this_thread::sleep_for(std::chrono::seconds(3));
  // }

} // namespace
