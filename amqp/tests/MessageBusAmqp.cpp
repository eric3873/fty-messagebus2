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
#include <fty/messagebus/MessageBus.h>
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

  static constexpr auto TEST_TOPIC = "examples";
  static constexpr auto TEST_QUEUE = "examples";

  using namespace fty::messagebus;
  using namespace fty::messagebus::test;
  using namespace fty::messagebus::amqp;
  using Message = fty::messagebus::amqp::AmqpMessage;

  static auto s_msgBus = MsgBusAmqp("TestCase", AMQP_SERVER_URI);

  // Replyer listener
  void replyerListener(const Message& message)
  {
    std::cout << "replyerListener: " << message.userData() << std ::endl;
    auto state = s_msgBus.sendRequestReply(message, message.userData() + OK);
    REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);
  }

  // Reponse listener
  void responseListener(Message message)
  {
    std::cout << "responseListener: " << message.userData() << std ::endl;
    REQUIRE(message.userData() == RESPONSE);
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  // TEST_CASE("Amqp identify implementation", "[identify]")
  // {
  //   std::cout << "Debut test" << std::endl;
  //   auto msgBus = MsgBusAmqp("TestCase", AMQP_SERVER_URI);
  //   std::cout << "Apres constructor" << std::endl;
  //   std::size_t found = msgBus.identify().find("Amqp");
  //   REQUIRE(found != std::string::npos);
  //   // std::cout << "fin test" << std::endl;

  //   //std::this_thread::sleep_for(std::chrono::seconds(1));
  //   //REQUIRE(true);
  // }

  // TEST_CASE("Amqp proton message", "[hide]")
  // {
  //   auto msgBus = MsgBusAmqp("AmqpMessageTestCase", AMQP_SERVER_URI);
  //   auto message = msgBus.buildMessage(TEST_QUEUE, QUERY);
  //   proton::message protonMessage = getAmqpMessageFromMsgBusAmqpMessage(message);
  //   std::cout << protonMessage << std::endl;

  //   //Test all properties
  //   REQUIRE(protonMessage.body() == QUERY);
  //   REQUIRE(protonMessage.user() == "AmqpMessageTestCase");

  //   std::string replyTo(TEST_QUEUE);
  //   replyTo.append(".");
  //   replyTo.append(proton::to_string(protonMessage.correlation_id()));
  //   std::cout << replyTo << std::endl;
  //   REQUIRE(protonMessage.reply_to() == replyTo);
  // }

  TEST_CASE("Amqp async request", "[sendRequest]")
  {
    //auto msgBus = MsgBusAmqp("AmqpAsyncRequestTestCase", AMQP_SERVER_URI);
    DeliveryState state;

    state = s_msgBus.registerRequestListener(TEST_QUEUE, replyerListener);
    REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);

    //std::this_thread::sleep_for(std::chrono::seconds(3));

    state = s_msgBus.sendRequest(TEST_QUEUE, QUERY, responseListener);
    REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  TEST_CASE("Amqp sync request", "[sendRequest]")
  {
    auto msgBus = MsgBusAmqp("AmqpSyncRequestTestCase", AMQP_SERVER_URI);

    DeliveryState state = msgBus.registerRequestListener(TEST_QUEUE, replyerListener);
    REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);

    // Send synchronous request
    Opt<Message> replyMsg = msgBus.sendRequest(TEST_QUEUE, QUERY, MAX_TIMEOUT);
    REQUIRE(replyMsg.has_value());
    REQUIRE(replyMsg.value().userData() == RESPONSE);

    // replyMsg = msgBus.sendRequest(TEST_QUEUE, QUERY_2, MAX_TIMEOUT);
    // REQUIRE(replyMsg.has_value());
    // REQUIRE(replyMsg.value().userData() == RESPONSE_2);
  }

  // TEST_CASE("Amqp publish subscribe", "[hide]")
  // {
  //   auto msgBus = MsgBusAmqp("AmqpPubSubTestCase", AMQP_SERVER_URI);
  //   DeliveryState state;

  //   // TODO see only for subscribing
  //   state = msgBus.subscribe(TEST_TOPIC, {});
  //   REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);

  //   state = msgBus.publish(TEST_TOPIC, RESPONSE);
  //   REQUIRE(state == DeliveryState::DELI_STATE_ACCEPTED);
  //   // Wait to process publish
  //   std::this_thread::sleep_for(std::chrono::seconds(3));
  // }

} // namespace
