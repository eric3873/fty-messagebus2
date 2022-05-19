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

#include <fmt/format.h>
#include <string>

namespace fty::messagebus2 {

enum class ComState
{
    Unknown       = 0,
    None          = 1,
    Connected     = 2,
    Lost          = 3,
    NoContact     = 4,
    ConnectFailed = 5,
    Closed        = 6,
    Undefined     = 7,
};

inline std::string to_string(const ComState& state)
{
    switch (state) {
        case ComState::Unknown:
            return "UNKNOWN";
        case ComState::None:
            return "NONE";
        case ComState::Connected:
            return "CONNECTED";
        case ComState::Lost:
            return "LOST";
        case ComState::NoContact:
            return "NO CONTACT";
        case ComState::ConnectFailed:
            return "CONNECTION FAILED";
        case ComState::Closed:
            return "CONNECTION CLOSED";
        default:
            break;
    }
    return "UNDEFINED";
}

inline ComState from_com_state(const std::string& state)
{
    if (state == to_string(ComState::Unknown)) {
        return ComState::Unknown;
    } else if (state == to_string(ComState::None)) {
        return ComState::None;
    } else if (state == to_string(ComState::Connected)) {
        return ComState::Connected;
    } else if (state == to_string(ComState::Lost)) {
        return ComState::Lost;
    } else if (state == to_string(ComState::NoContact)) {
        return ComState::NoContact;
    } else if (state == to_string(ComState::ConnectFailed)) {
        return ComState::ConnectFailed;
    } else if (state == to_string(ComState::Closed)) {
        return ComState::Closed;
    } else {
        return ComState::Undefined;
    }
}

enum class DeliveryState
{
    Unknown      = 0,
    Accepted     = 1,
    Rejected     = 2,
    Timeout      = 3,
    NotSupported = 4,
    Pending      = 5,
    Busy         = 6,
    Aborted      = 7,
    Unavailable  = 9,
    Undefined    = 10
};

inline std::string to_string(const DeliveryState& state)
{
    switch (state) {
        case DeliveryState::Unknown:
            return "UNKNOWN";
        case DeliveryState::Accepted:
            return "ACCEPTED";
        case DeliveryState::Rejected:
            return "REJECTED";
        case DeliveryState::Timeout:
            return "TIMEOUT";
        case DeliveryState::NotSupported:
            return "NOT SUPPORTED";
        case DeliveryState::Pending:
            return "PENDING";
        case DeliveryState::Busy:
            return "BUSY";
        case DeliveryState::Aborted:
            return "ABORTED";
        case DeliveryState::Unavailable:
            return "SERVICE UNAVAILABLE";
        default:
            break;
    }
    return "UNDEFINED";
}

inline DeliveryState from_deliveryState(const std::string& deliveryState)
{
    if (deliveryState == to_string(DeliveryState::Unknown)) {
        return DeliveryState::Unknown;
    } else if (deliveryState == to_string(DeliveryState::Accepted)) {
        return DeliveryState::Accepted;
    } else if (deliveryState == to_string(DeliveryState::Rejected)) {
        return DeliveryState::Rejected;
    } else if (deliveryState == to_string(DeliveryState::Timeout)) {
        return DeliveryState::Timeout;
    } else if (deliveryState == to_string(DeliveryState::NotSupported)) {
        return DeliveryState::NotSupported;
    } else if (deliveryState == to_string(DeliveryState::Pending)) {
        return DeliveryState::Pending;
    } else if (deliveryState == to_string(DeliveryState::Busy)) {
        return DeliveryState::Busy;
    } else if (deliveryState == to_string(DeliveryState::Aborted)) {
        return DeliveryState::Aborted;
    } else if (deliveryState == to_string(DeliveryState::Unavailable)) {
        return DeliveryState::Unavailable;
    } else {
        return DeliveryState::Undefined;
    }
}

} // namespace fty::messagebus2

inline std::ostream& operator<<(std::ostream& ss, fty::messagebus2::ComState value)
{
    ss << fty::messagebus2::to_string(value);
    return ss;
}

inline std::ostream& operator<<(std::ostream& ss, fty::messagebus2::DeliveryState value)
{
    ss << fty::messagebus2::to_string(value);
    return ss;
}

inline std::istream& operator>>(std::istream& ss, fty::messagebus2::ComState& value)
{
    std::string strval;
    ss >> strval;
    value = fty::messagebus2::from_com_state(strval);
    return ss;
}

inline std::istream& operator>>(std::istream& ss, fty::messagebus2::DeliveryState& value)
{
    std::string strval;
    ss >> strval;
    value = fty::messagebus2::from_deliveryState(strval);
    return ss;
}

/// Helper to format enums enity to fmt
template <typename T>
struct fmt::
    formatter<T, std::enable_if_t<std::is_same_v<fty::messagebus2::ComState, T> || std::is_same_v<fty::messagebus2::DeliveryState, T>, char>>
{
    template <typename ParseContext>
    constexpr auto parse(ParseContext& ctx)
    {
        return ctx.begin();
    }

    template <typename FormatContext>
    auto format(const T& attr, FormatContext& ctx)
    {
        return fmt::format_to(ctx.out(), "{}", fty::messagebus2::to_string(attr));
    }
};
