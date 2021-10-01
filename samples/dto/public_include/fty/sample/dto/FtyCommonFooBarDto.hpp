/*  =========================================================================
    fty_common_messagebus_dto - class description

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

#include <list>
#include <string>

namespace fty::sample::dto
{
  using UserData = std::string;

  struct FooBar
  {
    std::string foo;
    std::string bar;

    FooBar() = default;
    FooBar(const std::string& _foo, const std::string& _bar)
      : foo(_foo)
      , bar(_bar)
    {
    }

    FooBar(const std::string& input)
    {
      deserialize(input);
    }

    const std::string serialize() const;
    void deserialize(const std::string& input);
  };

  void operator<<(UserData& data, const FooBar& object);
  void operator>>(UserData& payload, FooBar& object);

} // namespace fty::messagebus::test
