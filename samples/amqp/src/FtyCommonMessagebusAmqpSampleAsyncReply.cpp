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

#include <fty/sample/dto/FtyCommonMathDto.h>
#include <fty/messagebus/amqp/MessageBusAmqp.h>

#include <csignal>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace
{
  using namespace fty::messagebus;
  using namespace fty::sample::dto;

  auto bus = amqp::MessageBusAmqp();
  static bool _continue = true;

  static void signalHandler(int signal)
  {
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
  }

  void replyerMessageListener(const Message& message)
  {
    logDebug("Message arrived: '{}'", message.toString());

    auto mathQuery = MathOperation(message.userData());
    auto mathResultResult = MathResult();

    if (mathQuery.operation == "add")
    {
      mathResultResult.result = mathQuery.param_1 + mathQuery.param_2;
    }
    else if (mathQuery.operation == "mult")
    {
      mathResultResult.result = mathQuery.param_1 * mathQuery.param_2;
    }
    else
    {
      mathResultResult.status = MathResult::STATUS_KO;
      mathResultResult.error = "Unsuported operation";
    }

    fty::Expected<Message> response = message.buildReply(mathResultResult.serialize());
    if (!response)
    {
      logError("Error while creating reply: {}", response.error());
      return;
    }

    fty::Expected<void> sendRet = bus.send(response.value());
    if (!sendRet)
    {
      logError("Error while sending: {}", sendRet.error());
      return;
    }

    //_continue = false;
  }

} // namespace

int main(int argc, char** argv)
{
  if (argc != 2)
  {
    std::cout << "USAGE: " << argv[0] << " <repQueue>" << std::endl;
    return EXIT_FAILURE;
  }

  logInfo("{} - starting...", argv[0]);

  auto address = std::string{argv[1]};

  // Install a signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  fty::Expected<void> connectionRet = bus.connect();
  if (!connectionRet)
  {
    logError("Error while connecting {}", connectionRet.error());
    return EXIT_FAILURE;
  }

  fty::Expected<void> subscribRet = bus.receive(address, replyerMessageListener);
  if (!subscribRet)
  {
    logError("Error while subscribing {}", subscribRet.error());
    return EXIT_FAILURE;
  }


  while (_continue)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  logInfo("{} - end", argv[0]);
  return EXIT_SUCCESS;
}
