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

#include "src/MsgBusAmqp.h"
#include <catch2/catch.hpp>
#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBusStatus.h>
#include <iostream>
#include <thread>

namespace {

#if defined(EXTERNAL_SERVER_FOR_TEST)
static constexpr auto AMQP_SERVER_URI{"x.x.x.x:5672"};
#else
static constexpr auto AMQP_SERVER_URI{"amqp://127.0.0.1:5672"};
#endif

using namespace fty::messagebus;

} // namespace

TEST_CASE("Amqp with no connection", "[MsgBusAmqp]")
{
    std::string topic = "topic://test.no.connection";
    Message     msg   = Message::buildMessage("AmqpNoConnectionTestCase", topic, "TEST", "QUERY");

    auto msgBus   = amqp::MsgBusAmqp("AmqpNoConnectionTestCase", AMQP_SERVER_URI);
    auto received = msgBus.receive(topic, {});
    REQUIRE(received.error() == DeliveryState::Unavailable);
    auto sent = msgBus.send(msg);
    REQUIRE(sent.error() == DeliveryState::Unavailable);
}

TEST_CASE("Amqp without and with connection", "[MsgBusAmqp]")
{
    auto msgBus = amqp::MsgBusAmqp("AmqpMessageBusStatusTestCase", AMQP_SERVER_URI);
    CHECK_FALSE(msgBus.isServiceAvailable());
    REQUIRE(msgBus.connect());
    REQUIRE(msgBus.isServiceAvailable());

    REQUIRE(msgBus.clientName() == "AmqpMessageBusStatusTestCase");
}
