/*  =========================================================================
    MsgBusAmqp.cpp - description

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

#define UNIT_TESTS

#include "fty/messagebus/test/MsgBusTestCommon.hpp"
#include <fty/messagebus/MsgBusAmqp.hpp>

#include <catch2/catch.hpp>

namespace
{
#if defined(EXTERNAL_SERVER_FOR_TEST)
  static constexpr auto AMQP_SERVER_URI{"x.x.x.x:5672"};
#else
  static constexpr auto AMQP_SERVER_URI{"127.0.0.1:5672"};
#endif

  using namespace fty::messagebus;
  using namespace fty::messagebus::test;
  using Message = fty::messagebus::amqp::AmqpMessage;

  static auto s_msgBus = MsgBusAmqp("TestCase", AMQP_SERVER_URI);

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------

  TEST_CASE("Amqp identify implementation", "[identify]")
  {
    std::size_t found = s_msgBus.identify().find("Amqp");
    REQUIRE(found != std::string::npos);
  }

} // namespace
