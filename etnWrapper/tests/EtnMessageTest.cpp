#include <etn/messagebus/EtnMessage.h>

#include <catch2/catch.hpp>
#include <iostream>

namespace
{

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------
  using namespace etn::messagebus;

  // TEST_CASE("Build etn message", "[Message][topic]")
  // {
  //   auto msg = EtnMessage::buildMessage("from", "to", "ETN_TEST_SUBJECT", "data");
  //   REQUIRE(msg.isValidMessage());
  //   REQUIRE(!msg.needReply());
  //   REQUIRE(msg.to().find("/etn/t/to") != std::string::npos);
  //   REQUIRE(msg.from() == "from");
  // }

  // TEST_CASE("Build etn request with reply", "[Message][queue]")
  // {
  //   auto msg = EtnMessage::buildRequest("from", "to", "ETN_TEST_SUBJECT", "myReply", "data");
  //   REQUIRE(msg.isValidMessage());
  //   REQUIRE(msg.isRequest());
  //   REQUIRE(msg.needReply());

  //   REQUIRE(msg.from().find("from") != std::string::npos);
  //   REQUIRE(msg.to().find("queue://etn.q.request.to") != std::string::npos);
  //   REQUIRE(msg.replyTo().find("queue://etn.q.reply.myReply") != std::string::npos);

  //   auto reply = msg.buildReply("data reply");
  //   REQUIRE(reply);
  //   REQUIRE(reply->isValidMessage());
  //   REQUIRE(!reply->needReply());
  //   REQUIRE(reply->correlationId() == msg.correlationId());
  //   REQUIRE(reply->from() == msg.to());
  //   REQUIRE(reply->to() == msg.replyTo());
  //   REQUIRE(reply->subject() == msg.subject());
  // }

  TEST_CASE("Build etn address", "[Message][queue][topic]")
  {
    REQUIRE(EtnMessage::buildAddress("myAddress").find("/etn/t/myAddress") != std::string::npos);
    REQUIRE(EtnMessage::buildAddress("myAddress", AddressType::TOPIC).find("/etn/t/myAddress") != std::string::npos);
    REQUIRE(EtnMessage::buildAddress("myAddress", AddressType::REQUEST_QUEUE).find("queue://etn.q.request.myAddress") != std::string::npos);
    REQUIRE(EtnMessage::buildAddress("myAddress", AddressType::REPLY_QUEUE).find("queue://etn.q.reply.myAddress") != std::string::npos);
  }
}
