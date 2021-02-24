package cz.o2.proxima.scheme;

import static org.junit.Assert.assertEquals;

import cz.o2.proxima.scheme.SchemaDescriptors.AttributeValue;
import cz.o2.proxima.scheme.SchemaDescriptors.PrimitiveTypeDescriptor;
import org.junit.Test;

public class SchemaDescriptorsAttributeValueTest {


  @Test
  public void testCreatePrimitiveValue() {
    PrimitiveTypeDescriptor<String> schema = SchemaDescriptors.strings();
    AttributeValue<String> value = schema.valueOf("foo");
    assertEquals("foo", value.getValue());
  }

  public void testComplexValue() {

  }
}
