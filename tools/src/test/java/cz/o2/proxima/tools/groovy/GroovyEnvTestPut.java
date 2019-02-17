package cz.o2.proxima.tools.groovy;

import com.google.gson.JsonElement;
import com.typesafe.config.ConfigFactory;
import cz.o2.proxima.repository.AttributeDescriptor;
import cz.o2.proxima.repository.ConfigRepository;
import cz.o2.proxima.repository.EntityDescriptor;
import cz.o2.proxima.scheme.proto.test.Scheme;
import cz.o2.proxima.storage.StreamElement;
import cz.o2.proxima.util.Optionals;
import groovy.lang.Script;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class GroovyEnvTestPut extends GroovyEnvAbstractPutTest {

  final EntityDescriptor event = Optionals.get(repo.findEntity("event"));
  final AttributeDescriptor<Scheme.Event> eventData = Optionals.get(
      event.findAttribute("data"));

  @BeforeClass
  public static void suitSetup() {
    cfg = ConfigFactory.load("reference.conf").resolve();
    repo = ConfigRepository.of(cfg);
  }

  @Ignore("Not implemented yet.")
  @Test
  public void testPutProtoFromJson() throws Exception {
    long now = System.currentTimeMillis();
    executeTest(new AbstractTest() {
      @Override
      Script inputScript() throws Exception {
        return compile("env.event.data.put(\"my-event\","
            + "\"{\\\"gatewayId\\\":\\\"test-gateway\\\","
            + "\\\"payload\\\": \\\"my-payload\\\"}\")");
      }

      @Override
      void validate(StreamElement element) {
        assertTrue(element.getStamp() > now);
        assertNotNull(element.getValue());
      }
    }, eventData);
  }
}
