/*
 * Copyright 2017-2024 O2 Czech Republic, a.s.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package cz.o2.proxima.direct.sql;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;

class FilterUtil {

  static List<String> extractKeysFromFilters(List<RexNode> filters) {
    List<String> keys = new ArrayList<>();
    for (RexNode filter : filters) {
      if (filter.isA(SqlKind.EQUALS)) {
        RexCall call = (RexCall) filter;
        if (call.operands.get(0) instanceof RexInputRef
            && call.operands.get(1) instanceof RexLiteral) {
          RexInputRef inputRef = (RexInputRef) call.operands.get(0);
          RexLiteral literal = (RexLiteral) call.operands.get(1);
          if (inputRef.getIndex() == 0) {
            keys.add(literal.getValueAs(String.class));
          }
        }
      }
    }
    return keys.isEmpty() ? null : keys;
  }

  private FilterUtil() {}
}
