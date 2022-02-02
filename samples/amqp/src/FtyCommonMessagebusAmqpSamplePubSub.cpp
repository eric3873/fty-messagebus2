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
#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/sample/dto/FtyCommonFooBarDto.h>
#include <fty_log.h>
#include <future>
#include <iostream>

namespace {

using namespace fty::messagebus;
using namespace fty::sample::dto;

static auto constexpr SAMPLE_TOPIC = "topic://etn.t.samples.pubsub";
// ensure that we received the message
static std::promise<bool> g_received;

static void signalHandler(int signal)
{
    std::cout << "Signal " << signal << " received\n";
    g_received.set_value(false);
}

void messageListener(const Message& message)
{
    logInfo("messageListener");
    auto metadata = message.metaData();
    for (const auto& pair : message.metaData()) {
        logInfo("  ** '{}' : '{}'", pair.first, pair.second);
    }

    auto fooBar = FooBar(message.userData());
    logInfo("  * foo    : '{}'", fooBar.foo);
    logInfo("  * bar    : '{}'", fooBar.bar);

    g_received.set_value(true);
}

} // namespace

int main(int /*argc*/, char** argv)
{
    logInfo("{} - starting...", argv[0]);

    // Install a signal handler
    std::signal(SIGINT, signalHandler);
    std::signal(SIGTERM, signalHandler);

    auto bus = amqp::MessageBusAmqp();

    auto connectionRet = bus.connect();
    if (!connectionRet) {
        logError("Error while connecting {}", to_string(connectionRet.error()));
        return EXIT_FAILURE;
    }

    auto subscribRet = bus.receive(SAMPLE_TOPIC, messageListener);
    if (!subscribRet) {
        logError("Error while subscribing {}", to_string(subscribRet.error()));
        return EXIT_FAILURE;
    }

    // Build message
    Message msg = Message::buildMessage(argv[0], SAMPLE_TOPIC, "PublishMessage", FooBar("event", "hello").serialize());

    // Send message
    auto sendRet = bus.send(msg);
    if (!sendRet) {
        logError("Error while sending {}", to_string(sendRet.error()));
        return EXIT_FAILURE;
    }

    g_received.get_future().get();
    logInfo("{} - end", argv[0]);
    return EXIT_SUCCESS;
}
