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

#include <chrono>
#include <ctime>

#include <random>
#include <string>
#include <uuid/uuid.h>

namespace fty::messagebus::utils
{
  const std::string generateUuid()
  {
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[UUID_STR_LEN + 1];
    uuid_unparse_lower(uuid, uuid_str);
    return uuid_str;
  }

  const std::string generateId()
  {
    std::random_device rd;
    std::mt19937 rng(rd());
    std::uniform_int_distribution<int> uni(0, RAND_MAX);
    return std::to_string(uni(rng));
  }

  const std::string getClientId(const std::string& prefix)
  {
    std::chrono::milliseconds ms = std::chrono::duration_cast<std::chrono::milliseconds>(
      std::chrono::system_clock::now().time_since_epoch());
    std::string clientId = prefix + "-" + std::to_string(ms.count());
    return clientId;
  }

} // namespace fty::messagebus::utils
