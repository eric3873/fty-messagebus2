/*  =========================================================================
    fty_common_messagebus_mqtt - class description

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

#ifndef FTY_COMMON_MESSAGEBUS_MQTT_CALL_BACK
#define FTY_COMMON_MESSAGEBUS_MQTT_CALL_BACK

#include "fty/messagebus/IMessageBus.hpp"
#include "fty/messagebus/mqtt/MsgBusMqttMessage.hpp"

#include <map>
#include <mqtt/async_client.h>
#include <string>
#include <thread>

namespace fty::messagebus::mqttv5
{

  using MessageListener = fty::messagebus::MessageListener<MqttMessage>;
  using subScriptionListener = std::map<std::string, MessageListener>;

  class CallBack : public virtual mqtt::callback
  {
  public:
    CallBack() = default;
    ~CallBack();
    void connection_lost(const std::string& cause) override;
    void onConnected(const std::string& cause);
    bool onConnectionUpdated(const mqtt::connect_data& connData);
    void onMessageArrived(mqtt::const_message_ptr msg);

    auto getSubscriptions() -> subScriptionListener;
    void setSubscriptions(const std::string& queue, MessageListener messageListener);

  private:
    subScriptionListener m_subscriptions;

    std::condition_variable_any m_cv;
    // TODO replace by a real thread pool
    std::vector<std::thread> m_threadPool;
  };
} // namespace fty::messagebus::mqttv5

#endif // ifndef FTY_COMMON_MESSAGEBUS_MQTT_CALL_BACK
