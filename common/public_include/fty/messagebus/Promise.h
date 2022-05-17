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

#pragma once

#include "fty/messagebus/Message.h"
#include "fty/messagebus/MessageBusStatus.h"
#include <map>
#include <string>
#include <future>
#include <thread>

namespace fty::messagebus {

class MessageBus;

// This class is a wrapper on Promise in order to ensure we unreceive when the object is destroyed
// std::promise and std::future are highly couple, using move constructor in this context is hard
// I chosed to use shared_ptr to simplify implementation
class Promise;
using PromisePtr = std::shared_ptr<Promise>;

class Promise
{
public:
    friend class MessageBus;

    //Remove copy constructors
    Promise& operator=(Promise& other) = delete;
    Promise(Promise& other) = delete;
    Promise& operator=(const Promise& other) = delete;
    Promise(const Promise& other) = delete;

    //Remove the move constructors
    Promise& operator=(Promise&& other) noexcept = delete;
    Promise(Promise&& other) noexcept = delete;


    std::future<Message>& getFuture();

    ~Promise();
    
private:
    MessageBus & m_messageBus; //message bus instance
    std::string m_queue;       //queue where the reply should arrive
    std::promise<Message> m_promise;
    std::future<Message> m_future;
    
    Promise(MessageBus & messageBus);
    void onReceive(const Message & msg);

};

} // namespace fty::messagebus
