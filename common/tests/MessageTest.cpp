#include <fty/messagebus/Message.h>

#include <catch2/catch.hpp>
#include <iostream>

namespace
{
  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------
  using namespace fty::messagebus;

  TEST_CASE("Build Message", "[Message]")
  {
    Message msg = Message::buildMessage("FROM", "Q.TO", "TEST_SUBJECT", "data");
    REQUIRE(msg.isValidMessage());
    REQUIRE(!msg.needReply());
  }

  TEST_CASE("Build Request with reply", "[Message]")
  {
    Message msg = Message::buildRequest("FROM", "Q.TO", "TEST_SUBJECT", "Q.REPLY", "data");
    REQUIRE(msg.isValidMessage());
    REQUIRE(msg.isRequest());
    REQUIRE(msg.needReply());

    auto reply = msg.buildReply("data reply");
    REQUIRE(reply);
    REQUIRE(reply->isValidMessage());
    REQUIRE(!reply->needReply());
  }

  TEST_CASE("Set message property", "[Message]")
  {
    Message msg;
    msg.setMetaDataValue("TEST", "test");
    REQUIRE(msg.getMetaDataValue("TEST") == "test");
    REQUIRE(msg.getMetaDataValue("UNKNOWN") == "");
  }
}