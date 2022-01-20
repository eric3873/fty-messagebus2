#include <fty/messagebus/Message.h>

#include <catch2/catch.hpp>
#include <iostream>

namespace
{
  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------
  using namespace fty::messagebus;

  TEST_CASE("Build message", "[Message]")
  {
    Message msg = Message::buildMessage("FROM", "Q.TO", "TEST_SUBJECT", "data");
    REQUIRE(msg.isValidMessage());
    REQUIRE(!msg.needReply());
  }

  TEST_CASE("Build request with reply", "[Message]")
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

  TEST_CASE("Build wrong message", "[Message]")
  {
    Message request = Message::buildRequest("", "", "", "", "data");
    auto replyData = request.userData() + "OK";

    REQUIRE(request.buildReply(replyData).error() == "Not a valid message!");
    request.from("from");
    REQUIRE(request.buildReply(replyData).error() == "Not a valid message!");
    request.to("Q.TO");
    REQUIRE(request.buildReply(replyData).error() == "Not a valid message!");
    request.subject("subject");
    REQUIRE(request.buildReply(replyData).error() == "No where to reply!");
    request.replyTo("Q.REPLY");
    REQUIRE(request.buildReply(replyData));

    request.toString();
  }

  TEST_CASE("Build message property", "[Message]")
  {
    Message msg = Message::buildMessage("FROM", "Q.TO", "TEST_SUBJECT", "data", {{STATUS, "myStatus"}, {MESSAGE_ID, "123456"}});
    REQUIRE(msg.userData() == "data");
    REQUIRE(msg.status() == "myStatus");
    REQUIRE(msg.id() == "123456");
    msg.setMetaDataValue("TEST", "test");
    REQUIRE(msg.getMetaDataValue("TEST") == "test");
    msg.id("1234567");
    REQUIRE(msg.id() == "1234567");
  }

}
