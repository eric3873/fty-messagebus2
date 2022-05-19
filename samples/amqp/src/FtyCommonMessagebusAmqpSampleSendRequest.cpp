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

#include <csignal>
#include <fty/messagebus2/amqp/MessageBusAmqp.h>
#include <fty/sample/dto/FtyCommonMathDto.h>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace {

using namespace fty::messagebus2;
using namespace fty::sample::dto;

static bool _continue                      = true;
static auto constexpr SYNC_REQUEST_TIMEOUT = 5;

static void signalHandler(int signal)
{
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
}

void responseMessageListener(const Message& message)
{
    logInfo("Response arrived: {}", message.toString());
    auto mathresult = MathResult(message.userData());
    logInfo("  * status: '{}', result: {}, error: '{}'", mathresult.status, mathresult.result, mathresult.error);

    _continue = false;
}

} // namespace

int main(int argc, char** argv)
{
    if (argc != 6) {
        std::cout << "USAGE: " << argv[0] << " <reqQueue, i.e. queue://etn.q.samples.maths/> <async|sync> <add|mult> <num1> <num2>"
                  << std::endl;
        return EXIT_FAILURE;
    }

    logInfo("{} - starting...", argv[0]);

    auto requestQueue = std::string{argv[1]};
    auto replyQueue   = requestQueue + ".reply";

    // Install a signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    auto bus = amqp::MessageBusAmqp(argv[0]);

    // Bus connection
    auto connectionRet = bus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", connectionRet.error());
        return EXIT_FAILURE;
    }

    auto operationQuery = MathOperation(argv[3], std::stoi(argv[4]), std::stoi(argv[5]));

    Message request =
        Message::buildRequest("MathsOperationsRequester", requestQueue, "MathsOperations", replyQueue, operationQuery.serialize());

    if (strcmp(argv[2], "sync") == 0) {
        _continue = false;

        auto reply = bus.request(request, SYNC_REQUEST_TIMEOUT);
        if (!reply) {
            std::cerr << "Error while requesting " << reply.error() << std::endl;
            return EXIT_FAILURE;
        }

        if (reply.value().status() != STATUS_OK) {
            std::cerr << "An error occured, message status is not OK!" << std::endl;
            return EXIT_FAILURE;
        }

        responseMessageListener(reply.value());
    } else {
        if (strcmp(argv[2], "async") == 0) {

            auto subscribRet = bus.receive(replyQueue, responseMessageListener, request.correlationId());
            if (!subscribRet) {
                logError("Error while subscribing {}", subscribRet.error());
                return EXIT_FAILURE;
            }
        } else {
            // Simple send message
            _continue = false;
        }

        auto sendRet = bus.send(request);
        if (!sendRet) {
            logError("Error while sending: {}", sendRet.error());
            return EXIT_FAILURE;
        }
    }

    while (_continue) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}
