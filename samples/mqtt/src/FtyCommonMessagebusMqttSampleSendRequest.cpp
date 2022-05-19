/*  =========================================================================
    FtyCommonMessagebusMqttSampleSendRequest - description

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

static bool _continue                            = true;
static auto constexpr SYNC_REQUEST_TIMEOUT       = 5;
static auto constexpr MATHS_OPERATOR_REPLY_QUEUE = "/etn/q/reply/maths/operator";

static void signalHandler(int signal)
{
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
}

void responseMessageListener(const Message& message)
{
    logInfo("Response arrived");
    auto mathresult = MathResult(message.userData());
    logInfo("  * status: '{}', result: %d, error: '{}'", mathresult.status.c_str(), mathresult.result, mathresult.error.c_str());

    _continue = false;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 6) {
        std::cout << "USAGE: " << argv[0] << " <reqQueue> <async|sync> <add|mult> <num1> <num2>" << std::endl;
        return EXIT_FAILURE;
    }

    logInfo("{} - starting...", argv[0]);

    auto requestQueue = std::string{argv[1]};

    // Install a signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    auto msgBus = mqtt::MessageBusMqtt();
    // Connect to the bus
    auto connectionRet = msgBus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", connectionRet.error());
        return EXIT_FAILURE;
    }

    auto query = MathOperation(argv[3], std::stoi(argv[4]), std::stoi(argv[5]));
    // Build the message to send
    Message msg = Message::buildRequest(argv[0], requestQueue, "mathQuery", MATHS_OPERATOR_REPLY_QUEUE, query.serialize());

    if (strcmp(argv[2], "async") == 0) {
        auto subscribRet = msgBus.receive(msg.replyTo(), responseMessageListener);
        if (!subscribRet) {
            logError("Error while subscribing {}", subscribRet.error());
            return EXIT_FAILURE;
        }
        auto sendRet = msgBus.send(msg);
        if (!sendRet) {
            logError("Error while sending {}", sendRet.error());
            return EXIT_FAILURE;
        }
    } else {
        _continue = false;

        auto replyMsg = msgBus.request(msg, SYNC_REQUEST_TIMEOUT);
        if (replyMsg) {
            responseMessageListener(replyMsg.value());
        } else {
            logError("Time out reached: (%ds)", SYNC_REQUEST_TIMEOUT);
        }
    }

    while (_continue) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}
