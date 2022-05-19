/*  =========================================================================
    FtyCommonMessagebusMqttSampleAsyncReply.cpp - description

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

#include "fty/sample/dto/FtyCommonMathDto.h"
#include <csignal>
#include <fty/messagebus2/MessageBus.h>
#include <fty/messagebus2/MessageBusStatus.h>
#include <fty/messagebus2/mqtt/MessageBusMqtt.h>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace {

using namespace fty::messagebus2;
using namespace fty::sample::dto;
static auto constexpr MATHS_OPERATOR_QUEUE = "/etn/q/request/maths/operator";

static bool _continue = true;

auto msgBus = mqtt::MessageBusMqtt();

static void signalHandler(int signal)
{
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
}

void replyerMessageListener(const Message& message)
{
    logInfo("Replyer messageListener");

    for (const auto& pair : message.metaData()) {
        logInfo("  ** '{}' : '{}'", pair.first.c_str(), pair.second.c_str());
    }

    auto mathQuery        = MathOperation(message.userData());
    auto mathResultResult = MathResult();

    if (mathQuery.operation == "add") {
        mathResultResult.result = mathQuery.param_1 + mathQuery.param_2;
    } else if (mathQuery.operation == "mult") {
        mathResultResult.result = mathQuery.param_1 * mathQuery.param_2;
    } else {
        mathResultResult.status = MathResult::STATUS_KO;
        mathResultResult.error  = "Unsuported operation";
    }

    // Build the response
    auto response = message.buildReply(mathResultResult.serialize());
    if (!response) {
        logError("Error on buildReply {}", response.error());
    }
    // send the response
    auto returnSend = msgBus.send(response.value());
    if (!returnSend) {
        logError("Error on send {}", returnSend.error());
    }
    //_continue = false;
}

} // namespace

int main(int /*argc*/, char** argv)
{
    logInfo("{} - starting...", argv[0]);

    // Install a signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    // Connect to the bus
    auto connectionRet = msgBus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", connectionRet.error());
        return EXIT_FAILURE;
    }

    auto subscribRet = msgBus.receive(MATHS_OPERATOR_QUEUE, replyerMessageListener);
    if (!subscribRet) {
        logError("Error while subscribing {}", subscribRet.error());
        return EXIT_FAILURE;
    }

    while (_continue) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}
