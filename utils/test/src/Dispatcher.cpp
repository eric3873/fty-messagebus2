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
#define CATCH_CONFIG_MAIN
#define CATCH_CONFIG_DISABLE_EXCEPTIONS

#include <fty/messagebus/utils/MsgBusDispatcher.hpp>
#include <catch2/catch.hpp>

#include <iostream>
#include <set>

TEST_CASE("Dispatcher")
{
    std::cerr << " * fty_common_messagebus_dispatcher: " << std::endl;

    using namespace fty::messagebus::utils;
    {
        // Four-function calculator test.
        std::cerr << "  - calculator: ";

        using CalculatorDispatcher = Dispatcher<std::string, std::function<int(int, int)>, std::function<int(const std::string&, int, int)>>;
        CalculatorDispatcher::Map calculatorMap {
            { "+", [](int a, int b) -> int { return a + b; }},
            { "-", [](int a, int b) -> int { return a - b; }},
            { "*", [](int a, int b) -> int { return a * b; }},
            { "/", [](int a, int b) -> int { return a / b; }},
        } ;

        CalculatorDispatcher calculator(calculatorMap);

        for (int b = 1; b < 10; b++) {
            for (int a = 1; a < 10; a++) {
                REQUIRE(calculator("+", a, b) == (a+b));
                REQUIRE(calculator("-", a, b) == (a-b));
                REQUIRE(calculator("*", a, b) == (a*b));
                REQUIRE(calculator("/", a, b) == (a/b));
            }
        }

        // Check what happens on unknown operator.
        REQUIRE_THROWS_AS((calculator("A", 2, 3) == 'A'), std::bad_function_call);

        std::cerr << "OK" << std::endl;
    }

    {
        // Translator test.
        std::cerr << "  - translator: ";

        using TranslatorDispatcher = Dispatcher<std::string, std::function<std::string()>, std::function<std::string(const std::string&)>>;
        TranslatorDispatcher::Map translatorMap {
            { "hello", []() -> std::string { return "bonjour"; }},
            { "goodbye", []() -> std::string { return "au revoir"; }},
        } ;

        TranslatorDispatcher translator(translatorMap,
            [](const std::string& word) { return "unknown word " + word; }
        );

        REQUIRE(translator("hello") == "bonjour");
        REQUIRE(translator("goodbye") == "au revoir");
        REQUIRE(translator("candy") == "unknown word candy");

        std::cerr << "OK" << std::endl;
    }
}
