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
#include <fty/messagebus2/mqtt/MessageBusMqtt.h>
#include <fty_log.h>
#include <future>
#include <iostream>

using namespace fty::messagebus2;

// ensure that we received the message
static std::promise<bool> g_received;

// Message Handler
void messageListener(Message message);

int main(int /*argc*/, char** argv)
{
    logInfo("{} - starting...", argv[0]);

    // Create the bus object
    auto bus = mqtt::MessageBusMqtt(argv[0]);

    // Connect to the bus
    auto connectionRet = bus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", connectionRet.error());
        return EXIT_FAILURE;
    }

    // Subscrib to the bus
    auto subscribRet = bus.receive("/etn/samples/publish", messageListener);
    if (!subscribRet) {
        logError("Error while subscribing {}", subscribRet.error());
        return EXIT_FAILURE;
    }

    // Build the message to send
    Message msg = Message::buildMessage(argv[0], "/etn/samples/publish", "MESSAGE", "This is my test message");

    // Send the message
    auto sendRet = bus.send(msg);
    if (!sendRet) {
        logError("Error while sending {}", sendRet.error());
        return EXIT_FAILURE;
    }

    // Wait until the second thread receive the message
    g_received.get_future().get();
    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}

void messageListener(Message message)
{
    logInfo("messageListener recieved: \n{}", message.toString().c_str());
    g_received.set_value(true);
}
