/*  =========================================================================
    fty_common_messagebus_message - class description

    Copyright (C) 2014 - 2020 Eaton

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

#ifndef FTY_COMMON_MESSAGEBUS_MQTT_MESSAGE_HPP
#define FTY_COMMON_MESSAGEBUS_MQTT_MESSAGE_HPP

#include "fty_common_messagebus_Imessage.hpp"

#include <list>

namespace fty::messagebus::mqttv5
{
  // Json representation
  using UserData = std::list<std::string>;

  class MqttMessage final : public IMessage<UserData>
  {
  public:
    MqttMessage() = default;
    MqttMessage(const MetaData& metaData, const UserData& userData = {});
    MqttMessage(const MetaData& metaData, const std::string& input);
    ~MqttMessage() = default;

    auto serialize() const -> std::string const;
    void deSerialize(const std::string& input);
  };

} // namespace messagebus::mqttv5

#endif // FTY_COMMON_MESSAGEBUS_MQTT_MESSAGE_HPP
