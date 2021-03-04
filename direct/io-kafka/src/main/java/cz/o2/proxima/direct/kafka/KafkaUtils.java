/**
 * Copyright 2017-2021 O2 Czech Republic, a.s.
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
package cz.o2.proxima.direct.kafka;

import cz.o2.proxima.util.ExceptionUtils;
import java.lang.reflect.Method;
import java.time.Duration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

class KafkaUtils {

  public static boolean waitForAssignment(
      KafkaConsumer<Object, Object> consumer, Duration timeout) {
    Timer timer = Time.SYSTEM.timer(timeout);
    Method method =
        ExceptionUtils.uncheckedFactory(
            () ->
                consumer
                    .getClass()
                    .getDeclaredMethod(
                        "updateAssignmentMetadataIfNeeded", Timer.class, boolean.class));
    method.setAccessible(true);
    return ExceptionUtils.uncheckedFactory(() -> (boolean) method.invoke(consumer, timer, true));
  }
}
