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

#include <fty/messagebus/MessageBus.h>

namespace fty::messagebus {

fty::Expected<PromisePtr, DeliveryState> MessageBus::requestAsync(const Message & msg) noexcept {
    PromisePtr myPromise(new Promise(*this));

    //Try to bind the function to the reply queue
    fty::Expected<void, DeliveryState> retReceive =  receive(
        msg.replyTo(),
        std::bind(&Promise::onReceive, myPromise.get(), std::placeholders::_1), //do not give the sharedPtr to the call back, otherwise we will never unreceive
        msg.correlationId()
        );

    if(!retReceive) {
        return fty::unexpected(retReceive.error());
    }

    //Binding is ok, save the queue to unreceive in case of issue
    myPromise->m_queue = msg.replyTo();

    //Try to send the message
    fty::Expected<void, DeliveryState> retSend = send(msg);
    if(!retSend) {
        return fty::unexpected(retSend.error());
    }

    return myPromise;
}
} // namespace fty::messagebus