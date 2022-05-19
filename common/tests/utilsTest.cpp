#include <catch2/catch.hpp>
#include <fty/messagebus2/utils.h>
#include <fty/string-utils.h>
#include <iostream>

//----------------------------------------------------------------------
// Test case
//----------------------------------------------------------------------
using namespace fty::messagebus2::utils;

TEST_CASE("Utils Uuid", "[utils]")
{
    auto uuid = generateUuid();
    REQUIRE(uuid.size() == 36);
    auto vector = fty::split(uuid, "-");
    REQUIRE(vector.size() == 5);
    REQUIRE(vector.at(0).size() == 8);
    REQUIRE(vector.at(1).size() == 4);
    REQUIRE(vector.at(2).size() == 4);
    REQUIRE(vector.at(3).size() == 4);
    REQUIRE(vector.at(4).size() == 12);
}

TEST_CASE("Utils id", "[utils]")
{
    auto id = generateId();
    REQUIRE(id.size() > 0);
}

TEST_CASE("Utils clientId", "[utils]")
{
    auto clientId = getClientId("myPrefix");
    auto vector   = fty::split(clientId, "-");
    REQUIRE(vector.size() == 2);
    REQUIRE(vector.at(0) == "myPrefix");
    REQUIRE(vector.at(1).size() == 13);
}
