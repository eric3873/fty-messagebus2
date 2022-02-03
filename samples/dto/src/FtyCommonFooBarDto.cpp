/*  =========================================================================
    FtyCommonMessageBusDto - class description

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

/*
@header
    FtyCommonMessageBusDto -
@discuss
@end
*/

#include "fty/sample/dto/FtyCommonFooBarDto.h"
#include <nlohmann/json.hpp>
#include <ostream>

using json = nlohmann::json;

namespace fty::sample::dto {
const std::string FooBar::serialize() const
{
    json op;
    op["foo"] = foo;
    op["bar"] = bar;

    return op.dump();
}

void FooBar::deserialize(const std::string& input)
{
    auto jsonInput = json::parse(input);
    foo            = jsonInput["foo"];
    bar            = jsonInput["bar"];
}

void operator<<(UserData& data, const FooBar& object)
{
    data = object.serialize();
}

void operator>>(UserData& data, FooBar& object)
{
    object = FooBar(data);
}
} // namespace fty::sample::dto
