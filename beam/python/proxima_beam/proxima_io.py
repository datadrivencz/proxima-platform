#!/usr/bin/env python

import typing 

from apache_beam.transforms.external import JavaJarExpansionService
from apache_beam.transforms.external import ExternalTransform
from apache_beam.transforms.external import NamedTupleBasedPayloadBuilder

ReadSchema = typing.NamedTuple(
    'ReadSchema',
    [('entity', str), ('attributes', typing.List[str]),
     ('type', str), ('startStamp', typing.Optional[int]),
     ('endStamp', typing.Optional[int]), ('stopAtCurrent', typing.Optional[bool])])


class Read(ExternalTransform):
  """
  External transform for reading from Proxima plaform.
  """

  # Returns the key/value data as raw byte arrays
  byte_array_deserializer = (
      'org.apache.kafka.common.serialization.ByteArrayDeserializer')

  allowed_types = ["batch-snapshot", "batch-updates", "stream", "stream-from-oldest"]
  
  URN = ('beam:transform:cz.o2.proxima.beam:read:v1')

  def __init__(
      self,
      read_type,
      proxima_jar,
      entity,
      attributes,
      stop_at_current=True,
      start_stamp=None,
      end_stamp=None,
      additional_args=None):
    """
    Initializes a read operation from Proxima.

    :param read_type: type of read operation. One from allowed_types
    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param attributes: names of attributes
    :param stop_at_current: if we should stop processing at current data    
    :param start_stamp: starting timestamp in millis
    :param end_stamp: ending timestamp in millis
    :param additional_args: additional arguments to the expansion service
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
                startStamp=start_stamp,
                endStamp=end_stamp,
                stopAtCurrent=stop_at_current)),
        JavaJarExpansionService(proxima_jar, append_args=additional_args))

class ReadBatchSnapshot(Read):
  """
  Read from batch snapshot.
  """
  
  def __init__(
      self,
      proxima_jar,
      entity,
      attributes,
      additional_args=None):
    """
    Initializes a read operation from Proxima.

    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param attributes: names of attributes
    :param additional_args: additional arguments to the expansion service
    """
    super().__init__(
        "batch-snapshot",
        proxima_jar,
        entity,
        attributes,
        additional_args=additional_args)


class ReadBatchUpdates(Read):
  """
  Read from batch updates.
  """
  
  def __init__(
      self,
      proxima_jar,
      entity,
      attributes,
      start_stamp=None,
      end_stamp=None,
      additional_args=None):
    """
    Initializes a read operation from Proxima.

    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param attributes: names of attributes
    :param start_stamp: starting timestamp in millis
    :param end_stamp: ending timestamp in millis
    :param additional_args: additional arguments to the expansion service
    """
    super().__init__(
        "batch-updates",
        proxima_jar,
        entity,
        attributes,
        start_stamp=start_stamp,
        end_stamp=end_stamp,
        additioanl_args=additional_args)

class ReadStream(Read):
  """
  Read from commit-log from current data.
  """
  
  def __init__(
      self,
      proxima_jar,
      entity,
      attributes,
      additional_args=None):
    """
    Initializes a read operation from Proxima.

    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param attributes: names of attributes
    :param additional_args: additional arguments to the expansion service
    """
    super().__init__(
        "stream",
        proxima_jar,
        entity,
        attributes,
        additional_args=additional_args)

class ReadStreamFromOldest(Read):
  """
  Read from commit-log from oldest data.
  """
  
  def __init__(
      self,
      proxima_jar,
      entity,
      attributes,
      stop_at_current=True,
      additional_args=None):
    """
    Initializes a read operation from Proxima.

    :param proxima_jar: path to the proxima expansion service with model jar
    :param entity: Name of entity
    :param attributes: names of attributes
    :param stop_at_current: True if we should stop at current data
    :param additional_args: additional arguments to the expansion service
    """
    super().__init__(
        "stream-from-oldest",
        proxima_jar,
        entity,
        attributes,
        stop_at_current=stop_at_current,
        additional_args=additional_args)
