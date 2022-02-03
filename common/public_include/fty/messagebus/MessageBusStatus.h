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

#include <string>

namespace fty::messagebus {

enum ComState : uint8_t
{
    COM_STATE_UNKNOWN        = 0,
    COM_STATE_NONE           = 1,
    COM_STATE_OK             = 2,
    COM_STATE_LOST           = 3,
    COM_STATE_NO_CONTACT     = 4,
    COM_STATE_CONNECT_FAILED = 5,
    COM_STATE_UNDEFINED      = 6,
};

inline std::string to_string(const ComState& state)
{
    switch (state) {
        case COM_STATE_UNKNOWN:
            return "UNKNOWN";
        case COM_STATE_NONE:
            return "NONE";
        case COM_STATE_OK:
            return "OK";
        case COM_STATE_LOST:
            return "LOST";
        case COM_STATE_NO_CONTACT:
            return "NO CONTACT";
        case COM_STATE_CONNECT_FAILED:
            return "CONNECTION FAILED";
        default:
            break;
    }
    return "UNDEFINED";
}

inline ComState from_com_state(const std::string& state)
{
    if (state == to_string(ComState::COM_STATE_UNKNOWN)) {
        return ComState::COM_STATE_UNKNOWN;
    } else if (state == to_string(ComState::COM_STATE_NONE)) {
        return ComState::COM_STATE_NONE;
    } else if (state == to_string(ComState::COM_STATE_OK)) {
        return ComState::COM_STATE_OK;
    } else if (state == to_string(ComState::COM_STATE_LOST)) {
        return ComState::COM_STATE_LOST;
    } else if (state == to_string(ComState::COM_STATE_NO_CONTACT)) {
        return ComState::COM_STATE_NO_CONTACT;
    } else if (state == to_string(ComState::COM_STATE_CONNECT_FAILED)) {
        return ComState::COM_STATE_CONNECT_FAILED;
    } else {
        return ComState::COM_STATE_UNDEFINED;
    }
}

enum DeliveryState : uint8_t
{
    DELIVERY_STATE_UNKNOWN       = 0,
    DELIVERY_STATE_ACCEPTED      = 1,
    DELIVERY_STATE_REJECTED      = 2,
    DELIVERY_STATE_TIMEOUT       = 3,
    DELIVERY_STATE_NOT_SUPPORTED = 4,
    DELIVERY_STATE_PENDING       = 5,
    DELIVERY_STATE_BUSY          = 6,
    DELIVERY_STATE_ABORTED       = 7,
    DELIVERY_STATE_UNAVAILABLE   = 9,
    DELIVERY_STATE_UNDEFINED     = 10
};

inline std::string to_string(const DeliveryState& state)
{
    switch (state) {
        case DELIVERY_STATE_UNKNOWN:
            return "UNKNOWN";
        case DELIVERY_STATE_ACCEPTED:
            return "ACCEPTED";
        case DELIVERY_STATE_REJECTED:
            return "REJECTED";
        case DELIVERY_STATE_TIMEOUT:
            return "TIMEOUT";
        case DELIVERY_STATE_NOT_SUPPORTED:
            return "NOT SUPPORTED";
        case DELIVERY_STATE_PENDING:
            return "PENDING";
        case DELIVERY_STATE_BUSY:
            return "BUSY";
        case DELIVERY_STATE_ABORTED:
            return "ABORTED";
        case DELIVERY_STATE_UNAVAILABLE:
            return "SERVICE UNAVAILABLE";
        default:
            break;
    }
    return "UNDEFINED";
}

inline DeliveryState from_deliveryState(const std::string& deliveryState)
{
    if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_UNKNOWN)) {
        return DeliveryState::DELIVERY_STATE_UNKNOWN;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_ACCEPTED)) {
        return DeliveryState::DELIVERY_STATE_ACCEPTED;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_REJECTED)) {
        return DeliveryState::DELIVERY_STATE_REJECTED;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_TIMEOUT)) {
        return DeliveryState::DELIVERY_STATE_TIMEOUT;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_NOT_SUPPORTED)) {
        return DeliveryState::DELIVERY_STATE_NOT_SUPPORTED;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_PENDING)) {
        return DeliveryState::DELIVERY_STATE_PENDING;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_BUSY)) {
        return DeliveryState::DELIVERY_STATE_BUSY;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_ABORTED)) {
        return DeliveryState::DELIVERY_STATE_ABORTED;
    } else if (deliveryState == to_string(DeliveryState::DELIVERY_STATE_UNAVAILABLE)) {
        return DeliveryState::DELIVERY_STATE_UNAVAILABLE;
    } else {
        return DeliveryState::DELIVERY_STATE_UNDEFINED;
    }
}

} // namespace fty::messagebus
