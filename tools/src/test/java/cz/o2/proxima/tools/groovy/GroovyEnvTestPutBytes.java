/**
 * Copyright 2017-2019 O2 Czech Republic, a.s.
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
package cz.o2.proxima.tools.groovy;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import freemarker.template.Configuration;
import freemarker.template.TemplateExceptionHandler;
import groovy.lang.GroovyClassLoader;
import groovy.lang.Script;
import lombok.extern.slf4j.Slf4j;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.*;

@Slf4j
public class GroovyEnvTestPutBytes extends GroovyEnvAbstractPutTest {


  final EntityDescriptor gateway = Optionals.get(repo.findEntity("gateway"));
  final AttributeDescriptor<byte[]> users = Optionals.get(gateway.findAttribute("users"));
  final AttributeDescriptor<byte[]> device = Optionals.get(
      gateway.findAttribute("device.*"));


  @BeforeClass
  public static void suitSetup() {
    cfg = ConfigFactory.load("test-reference.conf").resolve();
    repo = ConfigRepository.of(cfg);
  }


  @Test
  public void testPutBytes() throws Exception {
    long now = System.currentTimeMillis();
    executeTest(new AbstractTest() {
      @Override
      Script inputScript() throws Exception {
        return compile("env.gateway.users.put(\"test-key\",\"testValue\".getBytes())");
      }

      @Override
      void validate(StreamElement element) {
        assertTrue(element.getStamp() > now);
        assertNotNull(element.getValue());
        assertEquals("testValue", new String(element.getValue()));
        assertEquals("test-key", element.getKey());
        assertEquals("users", element.getAttribute());
      }
    }, users);
  }

  @Test
  public void testPutBytesWithTimestamp() throws Exception {
    executeTest(new AbstractTest() {
      @Override
      Script inputScript() throws Exception {
        return compile("env.gateway.users.put(\"test-key\",123456789L,"
            + "\"testValue\".getBytes())");
      }

      @Override
      void validate(StreamElement element) {
        assertEquals(123456789L, element.getStamp());
        assertNotNull(element.getValue());
        assertEquals("testValue", new String(element.getValue()));
        assertEquals("test-key", element.getKey());
        assertEquals("users", element.getAttribute());
      }
    }, users);
  }

  @Test
  public void testPutBytesWildcard() throws Exception {
    final long start = System.currentTimeMillis();
    executeTest(new AbstractTest() {
      @Override
      Script inputScript() throws Exception {
        return compile("env.gateway.device.put(\"test-key\","
            + "\"toilet\",\"value\".getBytes())");
      }

      @Override
      void validate(StreamElement element) {
        assertTrue(element.getStamp() > start);
        assertNotNull(element.getValue());
        assertEquals("value", new String(element.getValue()));
        assertEquals("test-key", element.getKey());
        assertEquals("device.toilet", element.getAttribute());
      }
    }, device);
  }

  @Test
  public void testPutBytesWildcardWithTimestamp() throws Exception {
    final long tms = System.currentTimeMillis();
    executeTest(new AbstractTest() {
      @Override
      Script inputScript() throws Exception {
        return compile("env.gateway.device.put(\"test-key\",\"toilet\","
            + tms + ",\"value\".getBytes())");
      }

      @Override
      void validate(StreamElement element) {
        assertEquals(tms, element.getStamp());
        assertNotNull(element.getValue());
        assertEquals("value", new String(element.getValue()));
        assertEquals("test-key", element.getKey());
        assertEquals("device.toilet", element.getAttribute());
      }
    }, device);

  }


}
