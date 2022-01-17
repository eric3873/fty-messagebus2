#include <etn/messagebus/EtnMessage.h>

#include <catch2/catch.hpp>
#include <iostream>

namespace
{

  //----------------------------------------------------------------------
  // Test case
  //----------------------------------------------------------------------
  using namespace etn::messagebus;

  TEST_CASE("Build etn address", "[Message][queue][topic]")
  {
    REQUIRE(buildAddress("myAddress", AddressType::TOPIC).find("/etn/t/myAddress") != std::string::npos);
    REQUIRE(buildAddress("myAddress", AddressType::QUEUE).find("queue://") != std::string::npos);
    REQUIRE(buildAddress("myAddress", AddressType::REQUEST_QUEUE).find("queue://etn.q.myAddress.request") != std::string::npos);
    REQUIRE(buildAddress("myAddress", AddressType::REPLY_QUEUE).find("queue://etn.q.myAddress.reply") != std::string::npos);
  }
}
