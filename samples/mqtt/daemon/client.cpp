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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/mqtt/MessageBusMqtt.h>
#include <iostream>

using namespace fty::messagebus;

int main(int argc, char** argv)
{
    // Check the argument
    if (argc != 2) {
        std::cerr << argv[0] << " usage:" << std::endl;
        std::cerr << "\t" << argv[0] << " <string to change>" << std::endl;
        return EXIT_FAILURE;
    }

    auto bus = mqtt::MessageBusMqtt();

    // Connect to the bus
    fty::Expected<void> connectionRet = bus.connect();
    if (!connectionRet) {
        std::cerr << "Error while connecting " << connectionRet.error() << std::endl;
        return EXIT_FAILURE;
    }

    // Build the request --> TODO: We need a way to auto generate the reply queue
    Message request = Message::buildRequest(
        argv[0], "/etn/samples/daemon-basic/mailbox", "TO_UPPER", "/etn/samples/daemon-basic/reply/" + utils::generateId(), argv[1]);

    // Subscrib to the bus
    fty::Expected<Message> reply = bus.request(request, 1);
    if (!reply) {
        std::cerr << "Error while requesting " << reply.error() << std::endl;
        return EXIT_FAILURE;
    }

    if (reply.value().status() != STATUS_OK) {
        std::cerr << "An error occured, message status is not OK!" << std::endl;
        return EXIT_FAILURE;
    }

    // We print the result
    std::cout << reply.value().userData() << std::endl;

    return EXIT_SUCCESS;
}
