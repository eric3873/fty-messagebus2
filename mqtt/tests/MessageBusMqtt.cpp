/*  =========================================================================
    MessageBusMqtt.cpp - description

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

#include <fty/messagebus/mqtt/MessageBusMqtt.h>
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <fty/messagebus/Message.h>

#include <catch2/catch.hpp>
#include <iostream>

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

  static int MAX_TIMEOUT = 1;

  using namespace fty::messagebus;

  auto s_msgBus = mqtt::MessageBusMqtt("TestCase", MQTT_SERVER_URI);

  // Response listener
  void replyerAddOK(const Message& message)
  {
    std::cerr << "Message in replyerAddOK:\n" + message.toString() << std::endl;
    //Build the response
    auto response = message.buildReply(message.userData() + "OK");

    if(!response) {
      std::cerr << response.error() << std::endl;
    }

    //send the response
    auto returnVal = s_msgBus.send(response.value());
    if(!returnVal) {
      std::cerr << returnVal.error() << std::endl;
    }
  }

  void replyerTimeout(const Message& message)
  {
    std::cerr << "Message in replyerTimeout:\n" + message.toString() << std::endl;
    //Build the response
    auto response = message.buildReply(message.userData() + "OK");

    if(!response) {
      std::cerr << response.error() << std::endl;
    }

    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT+1));

    //send the response
    auto returnVal = s_msgBus.send(response.value());

    if(!returnVal) {
      std::cerr << returnVal.error() << std::endl;
    }
  }

  bool g_received = false;
  // message listener
  void messageListener(const Message& message)
  {
    std::cerr << "Message in message listener:\n" + message.toString() << std::endl;
    g_received = true;
  }

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Mqtt identify implementation", "[identity]")
  {
    REQUIRE(s_msgBus.identity() == mqtt::BUS_INDENTITY_MQTT);
  }

  TEST_CASE("Mqtt send", "[send]")
  {
    auto msgBus = mqtt::MessageBusMqtt("MqttMessageTestCase", MQTT_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto msgBus2 = mqtt::MessageBusMqtt("MqttMessageTestCase2", MQTT_SERVER_URI);
    auto returnVal2 = msgBus2.connect();
    REQUIRE(returnVal2);

    auto returnVal3 = msgBus2.subscribe("/test/message/send", messageListener);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message msg = Message::buildMessage("MqttMessageTestCase", "/test/message/send", "TEST", "test");
    std::cerr << "Message to send:\n" + msg.toString() << std::endl;

    g_received = false;

    auto returnVal4 = msgBus.send(msg);
    REQUIRE(returnVal4);
    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT));

    REQUIRE(g_received);
  }

  TEST_CASE("Mqtt request sync", "[request]")
  {
    auto msgBus = mqtt::MessageBusMqtt("MqttSyncRequestTestCase", MQTT_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto returnVal2 = s_msgBus.connect();
    REQUIRE(returnVal2);

    auto returnVal3 = s_msgBus.subscribe("/test/message/sync/request", replyerAddOK);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message request = Message::buildRequest("MqttSyncRequestTestCase", "/test/message/sync/request", "TEST", "/test/message/sync/response", "test:");
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.request(request, MAX_TIMEOUT);
    REQUIRE(replyMsg);
    REQUIRE(replyMsg.value().userData() == "test:OK");
  }

  TEST_CASE("Mqtt request sync timeout", "[request]")
  {
    auto msgBus = mqtt::MessageBusMqtt("MqttSyncRequestTestCase", MQTT_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto returnVal2 = s_msgBus.connect();
    REQUIRE(returnVal2);

    auto returnVal3 = s_msgBus.subscribe("/test/message/synctimeout/request", replyerTimeout);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message request = Message::buildRequest("MqttSyncRequestTestCase", "/test/message/synctimeout/request", "TEST", "/test/message/ssynctimeout/response", "test:");
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.request(request, MAX_TIMEOUT);
    REQUIRE(!replyMsg);
    REQUIRE(from_deliveryState_state(replyMsg.error()) == DeliveryState::DELIVERY_STATE_TIMEOUT);
  }

  TEST_CASE("Mqtt async request", "[send]")
  {
    auto msgBus = mqtt::MessageBusMqtt("MqttAsyncRequestTestCase", MQTT_SERVER_URI);
    auto returnVal1 = msgBus.connect();
    REQUIRE(returnVal1);

    auto returnVal2 = s_msgBus.connect();
    REQUIRE(returnVal2);

    auto returnVal3 = s_msgBus.subscribe("/test/message/async/request", replyerAddOK);
    REQUIRE(returnVal3);

    // Send synchronous request
    Message request = Message::buildRequest("MqttAsyncRequestTestCase", "/test/message/async/request", "TEST", "/test/message/async/reply", "test");
    std::cerr << "Request to send:\n" + request.toString() << std::endl;

    auto replyMsg = msgBus.send(request);
    REQUIRE(replyMsg);

    // Wait to process the response
    std::this_thread::sleep_for(std::chrono::seconds(MAX_TIMEOUT));
  }

} // namespace
