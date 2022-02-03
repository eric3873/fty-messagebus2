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
#include <fty/messagebus/Message.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>
#include <fty_log.h>
#include <future>
#include <iostream>

using namespace fty::messagebus;

// Use promise to exit
static std::promise<void> g_exit;

auto bus = mqtt::MessageBusMqtt();

void signalHandler(int signal);
void processMessage(const Message& message);

int main(int /*argc*/, char** argv)
{
    logInfo("{} - starting...", argv[0]);

    // Install a signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    // Connect to the bus
    auto connectionRet = bus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", connectionRet.error());
        return EXIT_FAILURE;
    }

    // Subscrib to the bus
    auto subscribRet = bus.receive("/etn/samples/daemon-basic/mailbox", processMessage);
    if (!subscribRet) {
        logError("Error while subscribing {}", subscribRet.error());
        return EXIT_FAILURE;
    }

    // Wait until we exit
    g_exit.get_future().get();
    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}

void signalHandler(int signal)
{
    std::cout << "Signal " << signal << " received\n";
    g_exit.set_value();
}

void processMessage(const Message& message)
{
    logInfo("Process message:\n {}", message.toString());

    // check the message has the good subject
    if (message.subject() != "TO_UPPER") {
        logError("Subject not supported");
        return;
    }

    // we process the message - Here nothing to do because
    std::string data = message.userData();

    std::transform(data.begin(), data.end(), data.begin(), [](unsigned char c) {
        return std::toupper(c);
    });

    fty::Expected<Message> response = message.buildReply(data);
    if (!response) {
        logError("Error while creating reply: {}", response.error());
        return;
    }

    // Send the message
    auto sendRet = bus.send(response.value());
    if (!sendRet) {
        logError("Error while sending: {}", sendRet.error());
        return;
    }
}
