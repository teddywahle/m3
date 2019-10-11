	"strings"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	xdebug "github.com/m3db/m3/src/x/debug"
	xdocs "github.com/m3db/m3/src/x/docs"
	"github.com/m3db/m3/src/x/lockfile"
	"github.com/m3db/m3/src/x/mmap"
	xos "github.com/m3db/m3/src/x/os"
	"github.com/m3db/m3/src/x/serialize"
	apachethrift "github.com/apache/thrift/lib/go/thrift"
	opentracing "github.com/opentracing/opentracing-go"
	"go.uber.org/zap"
	cpuProfileDuration               = 5 * time.Second
	defaultServiceName               = "m3dbnode"
	skipRaiseProcessLimitsEnvVar     = "SKIP_PROCESS_LIMITS_RAISE"
	skipRaiseProcessLimitsEnvVarTrue = "true"
	defer logger.Sync()

	xconfig.WarnOnDeprecation(cfg, logger)

	// By default attempt to raise process limits, which is a benign operation.
	skipRaiseLimits := strings.TrimSpace(os.Getenv(skipRaiseProcessLimitsEnvVar))
	if skipRaiseLimits != skipRaiseProcessLimitsEnvVarTrue {
		// Raise fd limits to nr_open system limit
		result, err := xos.RaiseProcessNoFileToNROpen()
		if err != nil {
			logger.Warn("unable to raise rlimit", zap.Error(err))
		} else {
			logger.Info("raised rlimit no file fds limit",
				zap.Bool("required", result.RaisePerformed),
				zap.Uint64("sysNROpenValue", result.NROpenValue),
				zap.Uint64("noFileMaxValue", result.NoFileMaxValue),
				zap.Uint64("noFileCurrValue", result.NoFileCurrValue))
		}
	}
	// Parse file and directory modes
		logger.Fatal("could not parse new file mode", zap.Error(err))
		logger.Fatal("could not parse new directory mode", zap.Error(err))
		logger.Fatal("could not acquire lock", zap.String("path", lockPath), zap.Error(err))
		logger.Fatal("could not connect to metrics", zap.Error(err))
		logger.Fatal("could not resolve local host ID", zap.Error(err))
	var (
		tracer      opentracing.Tracer
		traceCloser io.Closer
	)

	if cfg.Tracing == nil {
		tracer = opentracing.NoopTracer{}
		logger.Info("tracing disabled; set `tracing.backend` to enable")
	} else {
		// setup tracer
		serviceName := cfg.Tracing.ServiceName
		if serviceName == "" {
			serviceName = defaultServiceName
		}
		tracer, traceCloser, err = cfg.Tracing.NewTracer(serviceName, scope.SubScope("jaeger"), logger)
		if err != nil {
			tracer = opentracing.NoopTracer{}
			logger.Warn("could not initialize tracing; using no-op tracer instead",
				zap.String("service", serviceName), zap.Error(err))
		} else {
			defer traceCloser.Close()
			logger.Info("tracing enabled", zap.String("service", serviceName))
		}
	}
		service, err := cfg.EnvironmentConfig.Services.SyncCluster()
		if err != nil {
			logger.Fatal("invalid cluster configuration", zap.Error(err))
		}

		clusters := service.Service.ETCDClusters
				logger.Fatal("unable to create etcd clusters", zap.Error(err))
			zone := service.Service.Zone
			logger.Info("using seed nodes etcd cluster",
				zap.String("zone", zone), zap.Strings("endpoints", endpoints))
			service.Service.ETCDClusters = []etcd.ClusterConfig{etcd.ClusterConfig{
		logger.Info("resolving seed node configuration",
			zap.String("hostID", hostID), zap.Strings("seedNodeHostIDs", seedNodeHostIDs),
		)
				logger.Fatal("unable to create etcd config", zap.Error(err))
				logger.Fatal("could not start embedded etcd", zap.Error(err))
	var (
		opts  = storage.NewOptions()
		iopts = opts.InstrumentOptions().
			SetLogger(logger).
			SetMetricsScope(scope).
			SetMetricsSamplingRate(cfg.Metrics.SampleRate()).
			SetTracer(tracer)
	)
	// Only override the default MemoryTracker (which has default limits) if a custom limit has
	// been set.
	if cfg.Limits.MaxOutstandingRepairedBytes > 0 {
		memTrackerOptions := storage.NewMemoryTrackerOptions(cfg.Limits.MaxOutstandingRepairedBytes)
		memTracker := storage.NewMemoryTracker(memTrackerOptions)
		opts = opts.SetMemoryTracker(memTracker)
	}

	opentracing.SetGlobalTracer(tracer)

	debugWriter, err := xdebug.NewZipWriterWithDefaultSources(
		cpuProfileDuration,
		iopts,
	)
	if err != nil {
		logger.Error("unable to create debug writer", zap.Error(err))
	}

		logger.Warn("max index query IDs concurrency was not set, falling back to default value")
		logger.Fatal("unable to start build reporter", zap.Error(err))
		logger.Fatal("could not construct postings list cache", zap.Error(err))
		logger.Fatal("could not set initial runtime options", zap.Error(err))
			logger.Fatal("could not determine if host supports HugeTLB", zap.Error(err))
			logger.Warn("host doesn't support HugeTLB, proceeding without it")
	// Pass nil for block.LeaseVerifier for now and it will be set after the
	// db is constructed (since the db is required to construct a
	// block.LeaseVerifier). Initialized here because it needs to be propagated
	// to both the DB and the blockRetriever.
	blockLeaseManager := block.NewLeaseManager(nil)
	opts = opts.SetBlockLeaseManager(blockLeaseManager)
		SetForceBloomFilterMmapMemory(cfg.Filesystem.ForceBloomFilterMmapMemoryOrDefault()).
		SetIndexBloomFilterFalsePositivePercent(cfg.Filesystem.BloomFilterFalsePositivePercentOrDefault())
		logger.Fatal("unknown commit log queue size type",
			zap.Any("type", cfg.CommitLog.Queue.CalculationType))
			logger.Fatal("unknown commit log queue channel size type",
				zap.Any("type", cfg.CommitLog.Queue.CalculationType))
	opts = withEncodingAndPoolingOptions(cfg, logger, opts, cfg.PoolingPolicy)

			SetIdentifierPool(opts.IdentifierPool()).
			SetBlockLeaseManager(blockLeaseManager)
				retriever, err := fs.NewBlockRetriever(retrieverOpts, fsopts)
				if err != nil {
					return nil, err
				}
		logger.Fatal("could not create persist manager", zap.Error(err))
	if len(cfg.EnvironmentConfig.Statics) == 0 {
			InstrumentOpts:   iopts,
			HashingSeed:      cfg.Hashing.Seed,
			NewDirectoryMode: newDirectoryMode,
			logger.Fatal("could not initialize dynamic config", zap.Error(err))
			logger.Fatal("could not initialize static config", zap.Error(err))
	syncCfg, err := envCfg.SyncCluster()
	if err != nil {
		logger.Fatal("invalid cluster config", zap.Error(err))
	}
		runOpts.ClusterClientCh <- syncCfg.ClusterClient
	opts = opts.SetNamespaceInitializer(syncCfg.NamespaceInitializer)

	// Set tchannelthrift options.
	ttopts := tchannelthrift.NewOptions().
		SetClockOptions(opts.ClockOptions()).
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetTopologyInitializer(syncCfg.TopologyInitializer).
		SetIdentifierPool(opts.IdentifierPool()).
		SetTagEncoderPool(tagEncoderPool).
		SetTagDecoderPool(tagDecoderPool).
		SetMaxOutstandingWriteRequests(cfg.Limits.MaxOutstandingWriteRequests).
		SetMaxOutstandingReadRequests(cfg.Limits.MaxOutstandingReadRequests)
	// Start servers before constructing the DB so orchestration tools can check health endpoints
	// before topology is set.
	var (
		contextPool  = opts.ContextPool()
		tchannelOpts = xtchannel.NewDefaultChannelOptions()
		// Pass nil for the database argument because we haven't constructed it yet. We'll call
		// SetDatabase() once we've initialized it.
		service = ttnode.NewService(nil, ttopts)
	)
	tchannelthriftNodeClose, err := ttnode.NewServer(service,
		cfg.ListenAddress, contextPool, tchannelOpts).ListenAndServe()
		logger.Fatal("could not open tchannelthrift interface",
			zap.String("address", cfg.ListenAddress), zap.Error(err))
	defer tchannelthriftNodeClose()
	logger.Info("node tchannelthrift: listening", zap.String("address", cfg.ListenAddress))
	httpjsonNodeClose, err := hjnode.NewServer(service,
		cfg.HTTPNodeListenAddress, contextPool, nil).ListenAndServe()
	if err != nil {
		logger.Fatal("could not open httpjson interface",
			zap.String("address", cfg.HTTPNodeListenAddress), zap.Error(err))
	}
	defer httpjsonNodeClose()
	logger.Info("node httpjson: listening", zap.String("address", cfg.HTTPNodeListenAddress))

	if cfg.DebugListenAddress != "" {
		go func() {
			mux := http.DefaultServeMux
			if debugWriter != nil {
				if err := debugWriter.RegisterHandler(xdebug.DebugURL, mux); err != nil {
					logger.Error("unable to register debug writer endpoint", zap.Error(err))
				}

			if err := http.ListenAndServe(cfg.DebugListenAddress, mux); err != nil {
				logger.Error("debug server could not listen",
					zap.String("address", cfg.DebugListenAddress), zap.Error(err))
			} else {
				logger.Info("debug server listening",
					zap.String("address", cfg.DebugListenAddress),
				)
			}
		}()
	}

	topo, err := syncCfg.TopologyInitializer.Init()
		logger.Fatal("could not initialize m3db topology", zap.Error(err))
	}

	var protoEnabled bool
	if cfg.Proto != nil && cfg.Proto.Enabled {
		protoEnabled = true
	}
	schemaRegistry := namespace.NewSchemaRegistry(protoEnabled, logger)
	// For application m3db client integration test convenience (where a local dbnode is started as a docker container),
	// we allow loading user schema from local file into schema registry.
	if protoEnabled {
		for nsID, protoConfig := range cfg.Proto.SchemaRegistry {
			dummyDeployID := "fromconfig"
			if err := namespace.LoadSchemaRegistryFromFile(schemaRegistry, ident.StringID(nsID),
				dummyDeployID,
				protoConfig.SchemaFilePath, protoConfig.MessageName); err != nil {
				logger.Fatal("could not load schema from configuration", zap.Error(err))
			}
		}
	}

	origin := topology.NewHost(hostID, "")
	m3dbClient, err := newAdminClient(
		cfg.Client, iopts, syncCfg.TopologyInitializer, runtimeOptsMgr,
		origin, protoEnabled, schemaRegistry, syncCfg.KVStore, logger)
	if err != nil {
		logger.Fatal("could not create m3db client", zap.Error(err))
	mutableSegmentAlloc := index.NewBootstrapResultMutableSegmentAllocator(
		opts.IndexOptions())
	rsOpts := result.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetDatabaseBlockOptions(opts.DatabaseBlockOptions()).
		SetSeriesCachePolicy(opts.SeriesCachePolicy()).
		SetIndexMutableSegmentAllocator(mutableSegmentAlloc)
	var repairClients []client.AdminClient
	if cfg.Repair != nil && cfg.Repair.Enabled {
		repairClients = append(repairClients, m3dbClient)
	}
	if cfg.Replication != nil {
		for _, cluster := range cfg.Replication.Clusters {
			if !cluster.RepairEnabled {
				continue
			}
			// Pass nil for the topology initializer because we want to create
			// a new one for the cluster we wish to replicate from, not use the
			// same one as the cluster this node belongs to.
			var topologyInitializer topology.Initializer
			// Guaranteed to not be nil if repair is enabled by config validation.
			clientCfg := *cluster.Client
			clusterClient, err := newAdminClient(
				clientCfg, iopts, topologyInitializer, runtimeOptsMgr,
				origin, protoEnabled, schemaRegistry, syncCfg.KVStore, logger)
			if err != nil {
				logger.Fatal(
					"unable to create client for replicated cluster",
					zap.String("clusterName", cluster.Name), zap.Error(err))
			}
			repairClients = append(repairClients, clusterClient)
		}
	}
	repairEnabled := len(repairClients) > 0
	if repairEnabled {
		repairOpts := opts.RepairOptions().
			SetAdminClients(repairClients)

		if cfg.Repair != nil {
			repairOpts = repairOpts.
				SetResultOptions(rsOpts).
				SetDebugShadowComparisonsEnabled(cfg.Repair.DebugShadowComparisonsEnabled)
			if cfg.Repair.Throttle > 0 {
				repairOpts = repairOpts.SetRepairThrottle(cfg.Repair.Throttle)
			}
			if cfg.Repair.CheckInterval > 0 {
				repairOpts = repairOpts.SetRepairCheckInterval(cfg.Repair.CheckInterval)
			}

			if cfg.Repair.DebugShadowComparisonsPercentage > 0 {
				// Set conditionally to avoid stomping on the default value of 1.0.
				repairOpts = repairOpts.SetDebugShadowComparisonsPercentage(cfg.Repair.DebugShadowComparisonsPercentage)
			}
		}

		opts = opts.
			SetRepairEnabled(true).
			SetRepairOptions(repairOpts)
	} else {
		opts = opts.SetRepairEnabled(false)
	}
	bs, err := cfg.Bootstrap.New(config.NewBootstrapConfigurationValidator(),
		rsOpts, opts, topoMapProvider, origin, m3dbClient)
		logger.Fatal("could not create bootstrap process", zap.Error(err))

	bsGauge := instrument.NewStringListEmitter(scope, "bootstrappers")
	if err := bsGauge.Start(cfg.Bootstrap.Bootstrappers); err != nil {
		logger.Error("unable to start emitting bootstrap gauge",
			zap.Strings("bootstrappers", cfg.Bootstrap.Bootstrappers),
			zap.Error(err),
		)
	}
	defer func() {
		if err := bsGauge.Close(); err != nil {
			logger.Error("stop emitting bootstrap gauge failed", zap.Error(err))
		}
	}()

	kvWatchBootstrappers(syncCfg.KVStore, logger, timeout, cfg.Bootstrap.Bootstrappers,
				logger.Error("updated bootstrapper list is empty")
			updated, err := cfg.Bootstrap.New(config.NewBootstrapConfigurationValidator(),
				rsOpts, opts, topoMapProvider, origin, m3dbClient)
				logger.Error("updated bootstrapper list failed", zap.Error(err))

			if err := bsGauge.UpdateStringList(bootstrappers); err != nil {
				logger.Error("unable to update bootstrap gauge with new bootstrappers",
					zap.Strings("bootstrappers", bootstrappers),
					zap.Error(err),
				)
			}
	// Start the cluster services now that the M3DB client is available.
	tchannelthriftClusterClose, err := ttcluster.NewServer(m3dbClient,
		cfg.ClusterListenAddress, contextPool, tchannelOpts).ListenAndServe()
		logger.Fatal("could not open tchannelthrift interface",
			zap.String("address", cfg.ClusterListenAddress), zap.Error(err))
	defer tchannelthriftClusterClose()
	logger.Info("cluster tchannelthrift: listening", zap.String("address", cfg.ClusterListenAddress))
	httpjsonClusterClose, err := hjcluster.NewServer(m3dbClient,
		cfg.HTTPClusterListenAddress, contextPool, nil).ListenAndServe()
		logger.Fatal("could not open httpjson interface",
			zap.String("address", cfg.HTTPClusterListenAddress), zap.Error(err))
	defer httpjsonClusterClose()
	logger.Info("cluster httpjson: listening", zap.String("address", cfg.HTTPClusterListenAddress))
	// Initialize clustered database.
	clusterTopoWatch, err := topo.Watch()
		logger.Fatal("could not create cluster topology watch", zap.Error(err))
	opts = opts.SetSchemaRegistry(schemaRegistry)
	db, err := cluster.NewDatabase(hostID, topo, clusterTopoWatch, opts)
		logger.Fatal("could not construct database", zap.Error(err))
	// Now that the database has been created it can be set as the block lease verifier
	// on the block lease manager.
	leaseVerifier := storage.NewLeaseVerifier(db)
	blockLeaseManager.SetLeaseVerifier(leaseVerifier)
	if err := db.Open(); err != nil {
		logger.Fatal("could not open database", zap.Error(err))
	// Now that we've initialized the database we can set it on the service.
	service.SetDatabase(db)

			// Notify on bootstrap chan if specified.
		// Bootstrap asynchronously so we can handle interrupt.
			logger.Fatal("could not bootstrap database", zap.Error(err))
		logger.Info("bootstrapped")
		kvWatchNewSeriesLimitPerShard(syncCfg.KVStore, logger, topo,
	// Wait for process interrupt.
	xos.WaitForInterrupt(logger, xos.InterruptOptions{
		InterruptCh: runOpts.InterruptCh,
	})
	// Attempt graceful server close.
			logger.Error("close database error", zap.Error(err))
	// Wait then close or hard close.
		logger.Info("server closed")
		logger.Error("server closed after timeout", zap.Duration("timeout", closeTimeout))
func bgValidateProcessLimits(logger *zap.Logger) {
		logger.Warn("cannot validate process limits: invalid configuration found",
			zap.String("message", message))
		logger.Warn("invalid configuration found, refer to linked documentation for more information",
			zap.String("url", xdocs.Path("operational_guide/kernel_configuration")),
			zap.Error(err),
		)
	logger *zap.Logger,
			logger.Warn("error resolving cluster new series insert limit", zap.Error(err))
		logger.Warn("unable to set cluster new series insert limit", zap.Error(err))
		logger.Error("could not watch cluster new series insert limit", zap.Error(err))
					logger.Warn("unable to parse new cluster new series insert limit", zap.Error(err))
				logger.Warn("unable to set cluster new series insert limit", zap.Error(err))
	logger *zap.Logger,
	logger *zap.Logger,
		logger.Error("could not resolve KV", zap.String("key", key), zap.Error(err))
			logger.Error("could not unmarshal KV key", zap.String("key", key), zap.Error(err))
			logger.Error("could not process value of KV", zap.String("key", key), zap.Error(err))
			logger.Info("set KV key", zap.String("key", key), zap.Any("value", protoValue.Value))
		logger.Error("could not watch KV key", zap.String("key", key), zap.Error(err))
					logger.Warn("could not set default for KV key", zap.String("key", key), zap.Error(err))
				logger.Warn("could not unmarshal KV key", zap.String("key", key), zap.Error(err))
				logger.Warn("could not process change for KV key", zap.String("key", key), zap.Error(err))
			logger.Info("set KV key", zap.String("key", key), zap.Any("value", protoValue.Value))
	logger *zap.Logger,
		logger.Fatal("could not watch value for key with KV",
			zap.String("key", kvconfig.BootstrapperKey))
				logger.Error("error converting KV update to string array",
					zap.String("key", kvconfig.BootstrapperKey),
					zap.Error(err),
				)
	logger *zap.Logger,
	// Set the max bytes pool byte slice alloc size for the thrift pooling.
	thriftBytesAllocSize := policy.ThriftBytesPoolAllocSizeOrDefault()
	logger.Info("set thrift bytes pool alloc size",
		zap.Int("size", thriftBytesAllocSize))
	apachethrift.SetMaxBytesPoolAlloc(thriftBytesAllocSize)

		logger.Sugar().Infof("bytes pool registering bucket capacity=%d, size=%d, "+
			bucket.RefillLowWaterMarkOrDefault(), bucket.RefillHighWaterMarkOrDefault())
		logger.Fatal("unrecognized pooling type", zap.Any("type", policy.Type))
	{
		// Avoid polluting the rest of the function with `l` var
		l := logger
		if t := policy.Type; t != nil {
			l = l.With(zap.String("policy", string(*t)))
		}

		l.Info("bytes pool init")
		bytesPool.Init()
		l.Info("bytes pool init done")
	}
		if cfg.Proto != nil && cfg.Proto.Enabled {
	iteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		if cfg.Proto != nil && cfg.Proto.Enabled {
			return proto.NewIterator(r, descr, encodingOpts)
	multiIteratorPool.Init(func(r io.Reader, descr namespace.SchemaDescr) encoding.ReaderIterator {
		iter.Reset(r, descr)
	bucketPool := series.NewBufferBucketPool(
		poolOptions(policy.BufferBucketPool, scope.SubScope("buffer-bucket-pool")))
	bucketVersionsPool := series.NewBufferBucketVersionsPool(
		poolOptions(policy.BufferBucketVersionsPool, scope.SubScope("buffer-bucket-versions-pool")))

		SetWriteBatchPool(writeBatchPool).
		SetBufferBucketPool(bucketPool).
		SetBufferBucketVersionsPool(bucketVersionsPool)
		SetMultiReaderIteratorPool(multiIteratorPool).
		return block.NewDatabaseBlock(time.Time{}, 0, ts.Segment{}, blockOpts, namespace.Context{})
	// Set value transformation options.
	opts = opts.SetTruncateType(cfg.Transforms.TruncateBy)
	forcedValue := cfg.Transforms.ForcedValue
	if forcedValue != nil {
		opts = opts.SetWriteTransformOptions(series.WriteTransformOptions{
			ForceValueEnabled: true,
			ForceValue:        *forcedValue,
		})
	}

	// Set index options.
		SetAggregateResultsPool(aggregateQueryResultsPool).
		SetForwardIndexProbability(cfg.Index.ForwardIndexProbability).
		SetForwardIndexThreshold(cfg.Index.ForwardIndexThreshold)
func newAdminClient(
	config client.Configuration,
	iopts instrument.Options,
	topologyInitializer topology.Initializer,
	runtimeOptsMgr m3dbruntime.OptionsManager,
	origin topology.Host,
	protoEnabled bool,
	schemaRegistry namespace.SchemaRegistry,
	kvStore kv.Store,
	logger *zap.Logger,
) (client.AdminClient, error) {
	if config.EnvironmentConfig != nil {
		// If the user has provided an override for the dynamic client configuration
		// then we need to honor it by not passing our own topology initializer.
		topologyInitializer = nil
	}

	m3dbClient, err := config.NewAdminClient(
		client.ConfigurationParameters{
			InstrumentOptions: iopts.
				SetMetricsScope(iopts.MetricsScope().SubScope("m3dbclient")),
			TopologyInitializer: topologyInitializer,
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetRuntimeOptionsManager(runtimeOptsMgr).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetContextPool(opts.ContextPool()).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetOrigin(origin).(client.AdminOptions)
		},
		func(opts client.AdminOptions) client.AdminOptions {
			if protoEnabled {
				return opts.SetEncodingProto(encoding.NewOptions()).(client.AdminOptions)
			}
			return opts
		},
		func(opts client.AdminOptions) client.AdminOptions {
			return opts.SetSchemaRegistry(schemaRegistry).(client.AdminOptions)
		},
	)
	if err != nil {
		return nil, err
	}

	// Kick off runtime options manager KV watches.
	clientAdminOpts := m3dbClient.Options().(client.AdminOptions)
	kvWatchClientConsistencyLevels(kvStore, logger,
		clientAdminOpts, runtimeOptsMgr)
	return m3dbClient, nil
}
