# Copyright 2017-2023 O2 Czech Republic, a.s.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
 
from .proxima_io import Read

import os
import apache_beam as beam

from unittest import TestCase

JAR=os.getenv("EXPANSION_JAR")

class Test(TestCase):

  def testRead(self):
    with beam.Pipeline() as p:
      p | Read("batch-snapshot", JAR, "gateway", ["status"], additional_args=["--proxima_config", "test-reference.conf"]) | beam.CountByKey()
      p.run().waitUntilFinish()

