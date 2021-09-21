/*  =========================================================================
    ftyCommonMessagebusAmqpSamplePubSub - description

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

/*
@header
    ftyCommonMessagebusAmqpSamplePubSub -
@discuss
@end
*/

#include <fty/messagebus/amqp/MessageBusAmqp.h>
#include <fty/sample/dto/FtyCommonFooBarDto.hpp>

#include <csignal>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace
{
  using namespace fty::messagebus;
  using namespace fty::sample::dto;

  static auto constexpr SAMPLES_TOPIC = "/etn/samples/publish";

  static bool _continue = true;

  static void signalHandler(int signal)
  {
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
  }

  void messageListener(Message message)
  {
    logInfo("messageListener");
    auto metadata = message.metaData();
    for (const auto& pair : message.metaData())
    {
      logInfo("  ** '{}' : '{}'", pair.first.c_str(), pair.second.c_str());
    }

    auto fooBar = FooBar(message.userData());
    logInfo("  * foo    : '{}'", fooBar.foo.c_str());
    logInfo("  * bar    : '{}'", fooBar.bar.c_str());

    _continue = false;
  }

} // namespace

int main(int /*argc*/, char** argv)
{
  logInfo("{} - starting...", argv[0]);

  // Install a signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  auto bus = amqp::MessageBusAmqp();

  fty::Expected<void> connectionRet = bus.connect();
  if (!connectionRet)
  {
    logError("Error while connecting {}", connectionRet.error());
    return EXIT_FAILURE;
  }

  fty::Expected<void> subscribRet = bus.subscribe(SAMPLES_TOPIC, messageListener);
  if (!subscribRet)
  {
    logError("Error while subscribing {}", subscribRet.error());
    return EXIT_FAILURE;
  }

  std::this_thread::sleep_for(std::chrono::milliseconds(1));

  //Build the message to send
  Message msg = Message::buildMessage(argv[0], SAMPLES_TOPIC, "MESSAGE", FooBar("event", "hello").serialize());

  //Send the message
  fty::Expected<void> sendRet = bus.send(msg);
  if (!sendRet)
  {
    logError("Error while sending {}", sendRet.error());
    return EXIT_FAILURE;
  }
  //bus.publish(SAMPLE_TOPIC, FooBar("event", "hello").serialize());

  while (_continue)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  logInfo("{} - end", argv[0]);
  return EXIT_SUCCESS;
}
