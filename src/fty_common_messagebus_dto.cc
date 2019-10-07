/*  =========================================================================
    fty_common_messagebus_dto - class description

    Copyright (C) 2014 - 2019 Eaton

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
    fty_common_messagebus_dto -
@discuss
@end
*/

#include "fty_common_messagebus_classes.h"

void operator<< (messagebus::UserData &data, const FooBar &object) {
    data.push_back(object.foo);
    data.push_back(object.bar);
}

void operator>> (messagebus::UserData &data, FooBar &object) {
    auto foo = data.front();
    data.pop_front();
    auto bar = data.front();
    data.pop_front();
    object = FooBar(foo, bar);
}