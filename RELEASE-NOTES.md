 - Merge pull request [#977](https://github.com/datadrivencz/proxima-platform/pull/977): [beam] do not use String.format for logging errors in ProximaIO
 - Merge pull request [#976](https://github.com/datadrivencz/proxima-platform/pull/976): [compiler] remove GENERATED_FROM string from compiled model
 - Merge pull request [#975](https://github.com/datadrivencz/proxima-platform/pull/975): [infra] revert protobufs to 3.x
 - Merge pull request [#974](https://github.com/datadrivencz/proxima-platform/pull/974): [proxima-beam] bump beam to 2.64.0
 - Merge pull request [#973](https://github.com/datadrivencz/proxima-platform/pull/973): [beam-tools] add option to disable stable input for remote collect
 - Merge pull request [#972](https://github.com/datadrivencz/proxima-platform/pull/972): [beam-core] Use atomic references instead of volatile fields
 - Merge pull request [#971](https://github.com/datadrivencz/proxima-platform/pull/971): [direct-io-kafka] fix bounded read
 - Merge pull request [#970](https://github.com/datadrivencz/proxima-platform/pull/970): [proxima-beam-tools] fix JsonElement registration
 - Merge pull request [#969](https://github.com/datadrivencz/proxima-platform/pull/969): [direct-io-kafka] fix stopAtCurrent when reading topic
 - Merge pull request [#968](https://github.com/datadrivencz/proxima-platform/pull/968): [proxima-beam-tools] register json objects to kryo
 - Merge pull request [#967](https://github.com/datadrivencz/proxima-platform/pull/967): [direct-io-gcloud-storage] handle StorageExceptions with JsonResponse cause
 - Merge pull request [#966](https://github.com/datadrivencz/proxima-platform/pull/966): [direct-io-kafka] idle watermark robustness
 - Merge pull request [#965](https://github.com/datadrivencz/proxima-platform/pull/965): Fix zstd
 - Merge pull request [#963](https://github.com/datadrivencz/proxima-platform/pull/963): Kafka watermarking
 - Merge pull request [#962](https://github.com/datadrivencz/proxima-platform/pull/962): [proxima-direct-io-kafka] #350 additional fixes of watermarks
 - Merge pull request [#961](https://github.com/datadrivencz/proxima-platform/pull/961): [proxima-direct-io-kafka] #350 check endOffsets to mark partition idle
 - Merge pull request [#960](https://github.com/datadrivencz/proxima-platform/pull/960): [direct-io-cassandra] Upgrade CQL client
 - Merge pull request [#959](https://github.com/datadrivencz/proxima-platform/pull/959): [proxima-direct-core] fix LocalCachedPartitionedView.scanWildcard off-by-one error
 - Merge pull request [#958](https://github.com/datadrivencz/proxima-platform/pull/958): [proxima-direct-io-cassandra] unsynchronize CassandraWriter
 - Merge pull request [#957](https://github.com/datadrivencz/proxima-platform/pull/957): [proxima-direct-core] check prefix of cached scanWildcard's offset
 - Merge pull request [#956](https://github.com/datadrivencz/proxima-platform/pull/956): [ingest-server] strip '.*' from wildcard prefix
 - Merge pull request [#955](https://github.com/datadrivencz/proxima-platform/pull/955): [proxima-direct-ingest-server] fix single write ingest transactions
 - Merge pull request [#954](https://github.com/datadrivencz/proxima-platform/pull/954): [transaction-manager] compute transaction latency metric
 - Merge pull request [#953](https://github.com/datadrivencz/proxima-platform/pull/953): [proxima-direct-core] allow creation of committed transaction in caching
 - Merge pull request [#952](https://github.com/datadrivencz/proxima-platform/pull/952): [transactions] enforce timeout on sync()
 - Merge pull request [#951](https://github.com/datadrivencz/proxima-platform/pull/951): Revert "[proxima-beam-core] remove fork of CalendarWindows"
 - Merge pull request [#950](https://github.com/datadrivencz/proxima-platform/pull/950): [infra] upgrade setup-java to v4
 - Merge pull request [#949](https://github.com/datadrivencz/proxima-platform/pull/949): [infra] Happy new year!
 - Merge pull request [#948](https://github.com/datadrivencz/proxima-platform/pull/948): [proxima-direct-core] fix LocalCachedPartitionedView restart after error
 - Merge pull request [#947](https://github.com/datadrivencz/proxima-platform/pull/947): [proxima-beam-core] remove fork of CalendarWindows
 - Merge pull request [#946](https://github.com/datadrivencz/proxima-platform/pull/946): watermark shift
 - Merge pull request [#945](https://github.com/datadrivencz/proxima-platform/pull/945): [proxima-beam-core] #344 add FilterLatecomers transform
 - Merge pull request [#944](https://github.com/datadrivencz/proxima-platform/pull/944): [proxima-beam-core] state expander: drop elements with timestamp behind state write time
 - Merge pull request [#943](https://github.com/datadrivencz/proxima-platform/pull/943): [proxima-direct-core] Discard invalid cached response observer in TransactionResourceManager
 - Merge pull request [#942](https://github.com/datadrivencz/proxima-platform/pull/942): [proxima-beam-core] debugging external state expander
 - Merge pull request [#941](https://github.com/datadrivencz/proxima-platform/pull/941): [proxima-beam] bump beam to 2.61.0
 - Merge pull request [#940](https://github.com/datadrivencz/proxima-platform/pull/940): [tools] bump groovy to 4.0.24
 - Merge pull request [#939](https://github.com/datadrivencz/proxima-platform/pull/939): [proxima-beam-tools] fix logging of dynamic jar
 - Merge pull request [#938](https://github.com/datadrivencz/proxima-platform/pull/938): [proxima-beam-core] O2-Czech-Republic#339 expander fixes
 - Merge pull request [#937](https://github.com/datadrivencz/proxima-platform/pull/937): [proxima-beam] bump beam to 2.60.0
 - Merge pull request [#936](https://github.com/datadrivencz/proxima-platform/pull/936): [proxima-direct-core] O2-Czech-Republic#341 fix DUPLICATE response handling
 - Merge pull request [#935](https://github.com/datadrivencz/proxima-platform/pull/935): [proxima-direct] O2-Czech-Republic#341 fix transaction idempotency
 - Merge pull request [#934](https://github.com/datadrivencz/proxima-platform/pull/934): [proxima-direct-core] O2-Czech-Republic#340 do not terminate transaction after sync()
 - Merge pull request [#932](https://github.com/datadrivencz/proxima-platform/pull/932): [proxima-direct-cassandra] reinitialize cluster after failed session creation
 - Merge pull request [#931](https://github.com/datadrivencz/proxima-platform/pull/931): [proxima-beam] bump beam to 2.59.0
 - Merge pull request [#930](https://github.com/datadrivencz/proxima-platform/pull/930): [tools[ bump groovy to 4.0.23
 - Merge pull request [#929](https://github.com/datadrivencz/proxima-platform/pull/929): [proxima-direct-core] Allow committing transaction without updating
 - Merge pull request [#927](https://github.com/datadrivencz/proxima-platform/pull/927): [rpc] fix typo in comment
 - Merge pull request [#926](https://github.com/datadrivencz/proxima-platform/pull/926): [proxima-beam] bump beam to 2.58.0
 - Merge pull request [#925](https://github.com/datadrivencz/proxima-platform/pull/925): Bulk scanning
 - Merge pull request [#924](https://github.com/datadrivencz/proxima-platform/pull/924): [proxima-direct-transaction-manager] #337 ensure transaction server terminates on errors
 - Merge pull request [#923](https://github.com/datadrivencz/proxima-platform/pull/923): [ingest-client] add scanning to IngestClient
 - Merge pull request [#922](https://github.com/datadrivencz/proxima-platform/pull/922): [tools] bump groovy to 4.0.21
 - Merge pull request [#921](https://github.com/datadrivencz/proxima-platform/pull/921): [proxima-beam] bump beam to 2.57.0
 - Merge pull request [#920](https://github.com/datadrivencz/proxima-platform/pull/920): [proxima-direct-io-cassandra] small polishing
 - Merge pull request [#919](https://github.com/datadrivencz/proxima-platform/pull/919): [proxima-direct-io-cassandra] #331 fix deserialization of values written with serializer v2
 - Merge pull request [#918](https://github.com/datadrivencz/proxima-platform/pull/918): [proxima-direct-io-pubsub] verify pubsub bulks before writing
 - Merge pull request [#916](https://github.com/datadrivencz/proxima-platform/pull/916): [proxima-direct-transaction-manager] Add timeout for the server to terminate after error
 - Merge pull request [#915](https://github.com/datadrivencz/proxima-platform/pull/915): [docs] fix return value in observe() docs
 - Merge pull request [#914](https://github.com/datadrivencz/proxima-platform/pull/914): [proxima-direct-io-kafka] #334 add ValueAsStringSerializer
 - Merge pull request [#913](https://github.com/datadrivencz/proxima-platform/pull/913): [proxima-direct-io-pubsub] synchronize pubsub bulk periodic flush
 - Merge pull request [#912](https://github.com/datadrivencz/proxima-platform/pull/912): [docs] fix comments
 - Merge pull request [#909](https://github.com/datadrivencz/proxima-platform/pull/909): Pubsub bulk (#333)
 - Merge pull request [#911](https://github.com/datadrivencz/proxima-platform/pull/911): [infra] enable JUnit 5 tests
 - Merge pull request [#910](https://github.com/datadrivencz/proxima-platform/pull/910): [proxima] set LOG_LEVEL from env to tests
 - Merge pull request [#908](https://github.com/datadrivencz/proxima-platform/pull/908): [proxima-direct-transaction-manager] fix updating transaction with misssing wildcard attribute
 - Merge pull request [#907](https://github.com/datadrivencz/proxima-platform/pull/907): [docs] Add basic documentation for ACID transactions
 - Merge pull request [#906](https://github.com/datadrivencz/proxima-platform/pull/906): [docs] skeleton of documentation for BeamDataOperator
 - Merge pull request [#905](https://github.com/datadrivencz/proxima-platform/pull/905): [proxima-direct-core] add ProcessingTimeShiftingWatermarkIdlePolicy
 - Merge pull request [#904](https://github.com/datadrivencz/proxima-platform/pull/904): [proxima-beam] bump beam to 2.56.0
 - Merge pull request [#903](https://github.com/datadrivencz/proxima-platform/pull/903): [proxima-docs] add skeleton of docs for DirectDataOperator
 - Merge pull request [#902](https://github.com/datadrivencz/proxima-platform/pull/902): [transactions] enforce nonzero stamp in commit
 - Merge pull request [#900](https://github.com/datadrivencz/proxima-platform/pull/900): [proxima-direct-transaction] #329 commit via replication coordinator
 - Merge pull request [#901](https://github.com/datadrivencz/proxima-platform/pull/901): [proxima-beam-core] avoid ConcurrentModificationException in ProximaIO
 - Merge pull request [#898](https://github.com/datadrivencz/proxima-platform/pull/898): [proxima-beam] use ProximaIO for BeamStream#write
 - Merge pull request [#899](https://github.com/datadrivencz/proxima-platform/pull/899): [proxima-direct-transaction-manager] fix flaky test
 - Merge pull request [#897](https://github.com/datadrivencz/proxima-platform/pull/897): [proxima-core] Improve reporting of missing PRIMARY family for attribute
