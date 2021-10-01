/*  =========================================================================
    FtyCommonMqttTestMathDto - class description

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
  struct MathOperation
  {
    std::string operation;
    int param_1;
    int param_2;
    MathOperation() = default;
    MathOperation(const std::string& _operation, int _param_1, int _param_2)
      : operation(_operation)
      , param_1(_param_1)
      , param_2(_param_2)
    {
    }
    MathOperation(const std::string& input)
    {
      deserialize(input);
    }

    auto serialize() -> const std::string;
    void deserialize(const std::string& input);
  };

  struct MathResult
  {
    static auto constexpr STATUS_OK{"Ok"};
    static auto constexpr STATUS_KO{"KO"};

    std::string status = STATUS_OK;
    int result = 0;
    std::string error = "";

    MathResult() = default;
    MathResult(const std::string& _status, int _result, const std::string& _error)
      : status(_status)
      , result(_result)
      , error(_error)
    {
    }
    MathResult(const std::string& input)
    {
      deserialize(input);
    }

    auto serialize() -> const std::string;
    void deserialize(const std::string& input);
  };

} // namespace fty::messagebus::test
