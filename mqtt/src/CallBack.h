/*  =========================================================================
    CallBack.h - class description

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

#include <fty/messagebus/Message.h>
#include <fty/messagebus/MessageBus.h>
#include <fty/messagebus/utils/MsgBusPoolWorker.hpp>
#include <map>
#include <mqtt/async_client.h>
#include <mqtt/client.h>
#include <string>
#include <thread>

namespace fty::messagebus::mqtt {

using AsynClientPointer    = std::shared_ptr<::mqtt::async_client>;
using SynClientPointer     = std::shared_ptr<::mqtt::client>;
using MessageListener      = fty::messagebus::MessageListener;
using SubScriptionListener = std::map<std::string, MessageListener>;

using PoolWorkerPointer = std::shared_ptr<utils::PoolWorker>;

class CallBack : public ::mqtt::callback
{
public:
    CallBack();
    ~CallBack() = default;
    void connection_lost(const std::string& cause) override;
    void onMessageArrived(::mqtt::const_message_ptr msg, AsynClientPointer clientPointer = nullptr);

    SubScriptionListener subscriptions();
    void                 subscriptions(const std::string& topic, const MessageListener& messageListener);
    bool                 subscribed(const std::string& topic);
    void                 eraseSubscriptions(const std::string& topic);

private:
    SubScriptionListener m_subscriptions;
    PoolWorkerPointer    m_poolWorkers;
};

} // namespace fty::messagebus::mqtt
