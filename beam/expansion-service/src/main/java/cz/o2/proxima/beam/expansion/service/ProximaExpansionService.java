/*
 * Copyright 2017-2023 O2 Czech Republic, a.s.
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
package cz.o2.proxima.beam.expansion.service;

import cz.o2.proxima.typesafe.config.ConfigFactory;
import java.util.Arrays;
import org.apache.beam.sdk.expansion.service.ExpansionService;

public class ProximaExpansionService {
  public static void main(String[] args) throws Exception {
    String[] remainingArgs = args;
    if (args.length > 2 && args[args.length - 2].equals("--proxima_config")) {
      ConfigProvider.set(ConfigFactory.load(args[args.length - 1]).resolve());
      remainingArgs = Arrays.copyOfRange(args, 0, args.length - 2);
    }
    ExpansionService.main(remainingArgs);
  }
}
