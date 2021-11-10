/*  =========================================================================
    FtyCommonMessagebusMqttSampleDiscovery - description

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

#include <fty/messagebus/mqtt/MessageBusMqtt.h>

#include <csignal>
#include <fty_log.h>
#include <iostream>
#include <thread>

namespace
{
  using namespace fty::messagebus;

  static bool _continue = true;

  static void signalHandler(int signal)
  {
    std::cout << "Signal " << signal << " received\n";
    _continue = false;
  }
} // namespace

int main(int /*argc*/, char** argv)
{
  logInfo("{} - starting...", argv[0]);

  // Install a signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  auto msgBus = mqtt::MessageBusMqtt();
  //Connect to the bus
  fty::Expected<void> connectionRet = msgBus.connect();
  if (!connectionRet)
  {
    logError("Error while connecting {}", connectionRet.error());
    return EXIT_FAILURE;
  }

  while (_continue)
  {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  logInfo("{} - end", argv[0]);
  return EXIT_SUCCESS;
}
