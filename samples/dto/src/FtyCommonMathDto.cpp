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

/*
@header
    FtyCommonMqttTestMathDto -
@discuss
@end
*/

#include "fty/sample/dto/FtyCommonMathDto.h"

#include <nlohmann/json.hpp>
#include <ostream>

namespace fty::sample::dto
{
  using json = nlohmann::json;

  auto MathOperation::serialize() -> const std::string
  {
    json op;
    op["operation"] = operation;
    op["param_1"] = param_1;
    op["param_2"] = param_2;

    return op.dump();
  }

  void MathOperation::deserialize(const std::string& input)
  {
    auto jsonInput = json::parse(input);
    operation = jsonInput["operation"];
    param_1 = jsonInput["param_1"];
    param_2 = jsonInput["param_2"];
  }

  std::ostream& operator<<(std::ostream& os, const MathOperation& mathOperation)
  {
    os << "MathOperation(";
    os << "operation=" << mathOperation.operation;
    os << ", param_1=" << mathOperation.param_1;
    os << ", param_2=" << mathOperation.param_2;
    os << ")";
    return os;
  }

  auto MathResult::serialize() -> const std::string
  {
    json op;
    op["status"] = status;
    op["result"] = result;
    op["error"] = error;

    return op.dump();
  }

  void MathResult::deserialize(const std::string& input)
  {
    auto jsonInput = json::parse(input);
    status = jsonInput["status"];
    result = jsonInput["result"];
    error = jsonInput["error"];
  }

  std::ostream& operator<<(std::ostream& os, const MathResult& mathResult)
  {
    os << "MathResult(";
    os << "status=" << mathResult.status;
    os << ", result=" << mathResult.result;
    os << ", error=" << mathResult.error;
    os << ")";
    return os;
  }
} // namespace fty::messagebus::test
