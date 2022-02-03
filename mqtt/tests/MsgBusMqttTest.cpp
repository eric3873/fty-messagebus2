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

#include "src/MsgBusMqtt.h"
#include <catch2/catch.hpp>
#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <iostream>
#include <thread>

namespace {

#if defined(EXTERNAL_SERVER_FOR_TEST)
static constexpr auto MQTT_SERVER_URI{"tcp://mqtt.eclipse.org:1883"};
#else
static constexpr auto MQTT_SERVER_URI{"tcp://localhost:1883"};
#endif

using namespace fty::messagebus;
using namespace fty::messagebus::mqtt;

} // namespace

TEST_CASE("Mqtt with no connection", "[MsgBusMqtt]")
{
    std::string topic = "topic://test.no.connection";
    Message     msg   = Message::buildMessage("MqttNoConnectionTestCase", topic, "TEST", "QUERY");

    auto msgBus   = MsgBusMqtt("MqttNoConnectionTestCase", MQTT_SERVER_URI);
    auto received = msgBus.receive(topic, {});
    REQUIRE(received.error() == to_string(DeliveryState::Unavailable));
    auto sent = msgBus.send(msg);
    REQUIRE(sent.error() == to_string(DeliveryState::Unavailable));
}

TEST_CASE("Mqtt without and with connection", "[MsgBusMqtt]")
{
    auto msgBus = MsgBusMqtt("MqttMessageBusStatusTestCase", MQTT_SERVER_URI);
    CHECK_FALSE(msgBus.isServiceAvailable());
    auto connectionRet = msgBus.connect();
    REQUIRE(msgBus.connect());
    REQUIRE(msgBus.isServiceAvailable());

    REQUIRE(msgBus.clientName() == "MqttMessageBusStatusTestCase");
}
