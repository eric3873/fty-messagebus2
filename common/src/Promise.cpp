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
#include <fty/messagebus/Promise.h>
#include <fty/messagebus/MessageBus.h>
#include <iostream>

namespace fty::messagebus {

std::future<Message>& Promise::getFuture(){
    return std::ref(m_future);
}

Promise::~Promise() {
    if(!m_queue.empty()) {
        m_messageBus.unreceive(m_queue);
        m_queue = "";
    }
}

Promise::Promise(MessageBus & messageBus) : m_messageBus(messageBus){
    m_future = m_promise.get_future();
}

void Promise::onReceive(const Message & msg) {
    m_promise.set_value(msg);
}

} //namespace fty::messagebus 