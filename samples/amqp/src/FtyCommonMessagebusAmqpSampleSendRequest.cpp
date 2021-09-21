/*  =========================================================================
    ftyCommonMessagebusAmqpSampleSendRequest - description

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
    ftyCommonMessagebusAmqpSampleSendRequest -
@discuss
@end
*/

#include <fty/sample/dto/FtyCommonMathDto.hpp>
#include <fty/messagebus/amqp/MessageBusAmqp.h>

#include <csignal>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace
{
  using namespace fty::messagebus;
  using namespace fty::sample::dto;

  static bool _continue = true;
  static auto constexpr SYNC_REQUEST_TIMEOUT = 5;

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
  if (argc != 6)
  {
    std::cout << "USAGE: " << argv[0] << " <reqQueue, i.e. /etn/samples/maths/> <async|sync> <add|mult> <num1> <num2>" << std::endl;
    return EXIT_FAILURE;
  }

  logInfo("{} - starting...", argv[0]);

  auto requestQueue = std::string{argv[1]};
  auto replyQueue = requestQueue + "/reply/";

  // Install a signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  auto bus = amqp::MessageBusAmqp(argv[0]);

  //Connect to the bus
  fty::Expected<void> connectionRet = bus.connect();
  if (!connectionRet)
  {
    logError("Error while connecting {}", connectionRet.error());
    return EXIT_FAILURE;
  }

  auto operationQuery = MathOperation(argv[3], std::stoi(argv[4]), std::stoi(argv[5]));
  Message request = Message::buildRequest(argv[0], requestQueue, "MathsOperations", replyQueue + utils::generateId(), operationQuery.serialize());

  if (strcmp(argv[2], "async") == 0)
  {

    fty::Expected<void> subscribRet = bus.subscribe(replyQueue, responseMessageListener);
    if (!subscribRet)
    {
      logError("Error while subscribing {}", subscribRet.error());
      return EXIT_FAILURE;
    }

    fty::Expected<void> sendRet = bus.send(request);
    if (!sendRet)
    {
      logError("Error while sending: {}", sendRet.error());
      return EXIT_FAILURE;
    }
  }
  else
  {
    _continue = false;

    fty::Expected<Message> reply = bus.request(request, SYNC_REQUEST_TIMEOUT);
    if (!reply)
    {
      std::cerr << "Error while requesting " << reply.error() << std::endl;
      return EXIT_FAILURE;
    }

    if (reply.value().metaData().at(STATUS) != STATUS_OK)
    {
      std::cerr << "An error occured, message status is not OK!" << std::endl;
      return EXIT_FAILURE;
    }

    responseMessageListener(reply.value());
  }

  while (_continue)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  logInfo("{} - end", argv[0]);
  return EXIT_SUCCESS;
}
