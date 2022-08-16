/*  =========================================================================
    Copyright (C) 2014 - 2022 Eaton

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

#include <fty/expected.h>
#include <string>
#include <future>
#include <thread>

namespace fty::messagebus2 {

class Message;
class MessageBus;

// This class is a wrapper on secure Promise in order to ensure we unreceive when the object
// is destroyed.

////////////////////////////////////////////////////////////////////////////////
/// PromiseBase<T> class

template <typename T>
class PromiseBase
{
public:
    PromiseBase();
    ~PromiseBase();

    // Remove copy constructors
    PromiseBase<T>& operator = (PromiseBase& other) = delete;
    PromiseBase(PromiseBase& other) = delete;
    PromiseBase& operator = (const PromiseBase& other) = delete;
    PromiseBase(const PromiseBase& other) = delete;

    // Remove the move constructors
    PromiseBase& operator = (PromiseBase&& other) noexcept = delete;
    PromiseBase(PromiseBase&& other) noexcept = delete;

    std::future<T>& getFuture();
    bool isReady();
    void reset();
    bool waitFor(const int& timeout_ms);

protected:
    std::promise<T> m_promise;
    std::future<T>  m_future;
};

////////////////////////////////////////////////////////////////////////////////
/// Promise<T> class

template<typename T>
class Promise : public PromiseBase<T> {
public:
    Promise() : PromiseBase<T>() {}

    // Remove copy constructors
    Promise<T>& operator = (Promise& other) = delete;
    Promise(Promise& other) = delete;
    Promise& operator = (const Promise& other) = delete;
    Promise(const Promise& other) = delete;

    // Remove the move constructors
    Promise& operator = (Promise&& other) noexcept = delete;
    Promise(Promise&& other) noexcept = delete;

    fty::Expected<T> getValue();
    fty::Expected<void> setValue(const T& t);
};

////////////////////////////////////////////////////////////////////////////////
/// Promise<Message> class

template<>
class Promise<Message> : public PromiseBase<Message> {
public:
    friend class MessageBus;

    Promise(MessageBus& messageBus, const std::string& address = "", const std::string& filter = "");
    ~Promise();

    fty::Expected<Message> getValue();
    // TBD: caution message bus receive need void function
    //fty::Expected<void> setValue(const Message& m);
    void setValue(const Message& m);

protected:
    MessageBus& m_messageBus; // message bus instance
    std::string m_address;    // address where the reply should arrive
    std::string m_filter;     // filter use by the reply
};

using FunctionMessage = std::function<void(Message&)>;

////////////////////////////////////////////////////////////////////////////////
/// Promise<void> class

template<>
class Promise<void> : public PromiseBase<void> {
public:
    fty::Expected<void> getValue();
    fty::Expected<void> setValue();

};

template <typename T>
using PromisePtr = std::shared_ptr<Promise<T>>;

} // namespace fty::messagebus2