#!/usr/bin/env python

import typing 

from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

ReadSchema = typing.NamedTuple(
    'ReadSchema',
    [('entity', str), ('attributes', typing.List[str]),
     ('type', str), ('startStamp', typing.Optional[int]),
     ('endStamp', typing.Optional[int])])


class Read(ExternalTransform):
  """
    External transform for reading from Proxima plaform.
  """

  # Returns the key/value data as raw byte arrays
  byte_array_deserializer = (
      'org.apache.kafka.common.serialization.ByteArrayDeserializer')

  allowed_types = ["batch-snapshot"]
  
  URN = ('beam:transform:cz.o2.proxima.beam:read:v1')

  def __init__(
      self,
      read_type,
      proxima_jar,
      entity,
      attributes,
      startStamp=None,
      endStamp=None):
    """
    Initializes a read operation from Proxima.

    :param read_type: type of read operation. One from allowed_types
    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param startStamp: starting timestamp in millis
    :param endStamp: ending timestamp in millis
    """
    if read_type not in Read.allowed_types:
      raise ValueError(f"read_type should be one of {Read.allowed_types}, got {read_type}")

    super().__init__(
        self.URN,
        NamedTupleBasedPayloadBuilder(
            ReadSchema(
                type=read_type,
                entity=entity,
                attributes=attributes,
                startStamp=startStamp,
                endStamp=endStamp)),
        JavaJarExpansionService(proxima_jar))

