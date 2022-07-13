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

#include <fty/messagebus2/Promise.h>
#include <fty/messagebus2/MessageBus.h>

#include <iostream>

namespace fty::messagebus2 {

////////////////////////////////////////////////////////////////////////////////
/// PromiseBase<T> implementation

template <typename T>
PromiseBase<T>::PromiseBase() {
    m_future = m_promise.get_future();
}

template <typename T>
PromiseBase<T>::~PromiseBase() {
}

template <typename T>
std::future<T>& PromiseBase<T>::getFuture() {
    return std::ref(m_future);
}

template <typename T>
bool PromiseBase<T>::isReady() {
    bool res = false;
    if (m_future.valid()) {
        //auto r = m_future.wait_for(std::chrono::seconds(0));
        //res = (r == std::future_status::ready || r == std::future_status::timeout);
        res = true;
    }
    return res;
}

template <typename T>
bool PromiseBase<T>::waitFor(const int& timeout_ms) {
    return (
        isReady() &&
        m_future.wait_for(std::chrono::duration<int,std::milli>(timeout_ms)) == std::future_status::ready
    );
}

template <typename T>
void PromiseBase<T>::reset() {
    m_promise = std::promise<T>();
    m_future  = std::future<T>();
    m_future = m_promise.get_future();
}

////////////////////////////////////////////////////////////////////////////////
/// Promise<T> implementation

template <typename T>
fty::Expected<T> Promise<T>::getValue() {
    if (this->isReady()) {
        return this->m_future.get();
    }
    return fty::unexpected("Not ready");
}

template <typename T>
fty::Expected<void> Promise<T>::setValue(const T& t) {
    if (this->isReady()) {
        this->m_promise.set_value(t);
        return {};
    }
    return fty::unexpected("Not ready");
}

////////////////////////////////////////////////////////////////////////////////
/// Promise<Message> implementation

Promise<Message>::Promise(MessageBus& messageBus, const std::string& queue) :
    m_messageBus(messageBus),
    m_queue(queue) {
}

Promise<Message>::~Promise() {
    if(!m_queue.empty()) {
        m_messageBus.unreceive(m_queue);
        m_queue = "";
    }
}

fty::Expected<Message> Promise<Message>::getValue() {
    if (this->isReady()) {
        return this->m_future.get();
    }
    return fty::unexpected("Not ready");
}

//fty::Expected<void> Promise<Message>::setValue(Message& m) {
void Promise<Message>::setValue(const Message& m) {
    if (this->isReady()) {
        this->m_promise.set_value(m);
        //return {};
    }
    //return fty::unexpected("Not ready");
}

////////////////////////////////////////////////////////////////////////////////
/// Promise<void> implementation

fty::Expected<void> Promise<void>::getValue() {
    if (this->isReady()) {
        this->m_future.get();
        return {};
    }
    return fty::unexpected("Not ready");
}

fty::Expected<void> Promise<void>::setValue() {
    if (isReady()) {
        m_promise.set_value();
        return {};
    }
    return fty::unexpected("Not ready");
}

// needed for link
template class PromiseBase<void>;
template class PromiseBase<Message>;
template class PromiseBase<ComState>;

template class Promise<ComState>;

} //namespace fty::messagebus2