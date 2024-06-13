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
import java.util.Collections;
import java.util.List;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.checkerframework.checker.nullness.qual.Nullable;

class FilterUtil {

  static List<String> extractKeysFromFilters(@Nullable List<RexNode> filters) {
    if (filters == null) {
      return Collections.emptyList();
    }
    List<String> keys = new ArrayList<>();
    for (RexNode filter : filters) {
      extractKeys(filter, keys);
    }
    return keys.isEmpty() ? null : keys;
  }

  private static void extractKeys(RexNode filter, List<String> keys) {
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
    } else if (filter.isA(SqlKind.OR)) {
      RexCall call = (RexCall) filter;
      for (RexNode operand : call.getOperands()) {
        extractKeys(operand, keys);
      }
    } else if (filter.isA(SqlKind.SEARCH)) {
      RexCall call = (RexCall) filter;
      if (call.operands.get(0) instanceof RexInputRef
          && call.operands.get(1) instanceof RexLiteral) {
        RexInputRef inputRef = (RexInputRef) call.operands.get(0);
        RexLiteral literal = (RexLiteral) call.operands.get(1);
        if (inputRef.getIndex() == 0) {
          Object value = literal.getValue();
          if (value instanceof org.apache.calcite.util.Sarg) {
            org.apache.calcite.util.Sarg<?> sarg = (org.apache.calcite.util.Sarg<?>) value;
            if (sarg.isPoints()) {
              sarg.rangeSet
                  .asRanges()
                  .forEach(
                      r -> {
                        Object endpoint = r.lowerEndpoint();
                        if (endpoint instanceof org.apache.calcite.util.NlsString) {
                          keys.add(((org.apache.calcite.util.NlsString) endpoint).getValue());
                        } else {
                          keys.add(endpoint.toString());
                        }
                      });
            }
          }
        }
      }
    }
  }

  private FilterUtil() {}
}
