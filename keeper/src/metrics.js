const http = require('http');
const promClient = require('prom-client');
const { Server } = require('socket.io');
const { requireAdminAuth } = require('./auth');
const { URL } = require('url');
const { createLogger } = require('./logger');
const { ApiGateway } = require('./apiGateway');
const { FailurePredictor, KeeperReputationScorer } = require('./insights');
const SloMetrics = require('./sloMetrics');

class MetricsHistory {
  constructor(maxSamples = 120) {
    this.maxSamples = maxSamples;
    this.samples = [];
  }

  record(point) {
    this.samples.push({
      timestamp: new Date().toISOString(),
      ...point,
    });
    if (this.samples.length > this.maxSamples) {
      this.samples.shift();
    }
  }

  getSamples(limit) {
    const max = typeof limit === 'number' ? limit : this.samples.length;
    return this.samples.slice(-max);
  }
}

class Metrics {
  constructor() {
    this.startTime = Date.now();
    this.history = new MetricsHistory(
      parseInt(process.env.METRICS_HISTORY_MAX_SAMPLES || '120', 10),
    );
    this.maxFeeSamples = 100;
    this.lastPollAt = null;
    this.lastBacklogSize = null;
    this.retryPressure = 0;
    this.rpcConnected = false;
    this.adminState = { paused: false, reason: null, changedAt: null };
    this.shardState = {
      shardIndex: 0,
      shardCount: 1,
      shardLabel: 'shard-0',
      ownedTasks: 0,
      skippedTasks: 0,
    };
    this.dbShardState = {
      dbShardCount: 1,
      dbShardLabel: 'postgres-shard-0',
      dbShardStrategy: 'fixed',
      activeUsers: 0,
      pendingTasks: 0,
    };
    this.driftState = {
      warning: 0,
      critical: 0,
      maxDriftSeconds: 0,
      taskId: null,
      severity: 'none',
      observedAt: null,
    };
    this.failoverState = {
      activeIndex: 0,
      activeRegion: 'region-0',
      healthyEndpoints: 1,
      totalEndpoints: 1,
      endpoints: [],
    };
    this.reset();
  }

  reset() {
    this.counters = {
      tasksCheckedTotal: 0,
      tasksDueTotal: 0,
      tasksExecutedTotal: 0,
      tasksFailedTotal: 0,
      tasksSkippedIdempotencyTotal: 0,
      throttledRequestsTotal: 0,
      tasksSkippedIdempotencyTotal: 0,

      // SLO counters
      pollFreshnessSloSuccess: 0,
      pollFreshnessSloFailure: 0,
      executionTimelinessSloSuccess: 0,
      executionTimelinessSloFailure: 0,
      retriesExhausted: 0,
      retryAttemptsTotal: { success: 0, failure: 0, duplicate: 0 },

      // Retry-related counters
      retriesExecutedTotal: 0,
      retriesFailedTotal: 0,
      retriesExecutedTotal: 0,
      retriesFailedTotal: 0,
      adminStateChangesTotal: 0,
      webhookAcceptedTotal: 0,
      webhookRejectedTotal: 0,
      webhookReplayRejectedTotal: 0,
      failoverEventsTotal: 0,
      failoverSwitchesTotal: 0,
    };
    this.gauges = {
      avgFeePaidXlm: 0,
      lastCycleDurationMs: 0,
      rpcCircuitState: 0, // 0 = CLOSED, 1 = HALF_OPEN, 2 = OPEN

      // SLO gauges
      pollFreshnessSeconds: 0,
      oldestTaskAgeSeconds: 0,
      retryQueueSize: 0,
      pollFreshnessSloRate: 0,
      executionTimelinessSloRate: 0,
      lastRetryCycleDurationMs: 0,
      rpcCircuitState: 0,
      fraudRiskScore: 0,
      reconciliationBalanceDrift: 0,
      reconciliationPendingExecutions: 0,
    };
    this.feeSamples = [];
    this.fraudState = {
      observations: 0,
      alertsQueued: 0,
      alertsSent: 0,
      alertsSuppressed: 0,
      alertsFailed: 0,
      pipelineErrors: 0,
      lastRiskScore: 0,
      lastAlertAt: null,
      lastAlertReason: null,
      pendingAlerts: 0,
      recentObservations: 0,
    };
    this.reconciliationState = {
      reconciliations: 0,
      executionsObserved: 0,
      accountingChangesObserved: 0,
      matches: 0,
      mismatches: 0,
      pendingExecutions: 0,
      alertsQueued: 0,
      alertsSent: 0,
      alertsFailed: 0,
      pipelineErrors: 0,
      lastDrift: 0,
      lastMismatchAt: null,
      lastMismatchReason: null,
      lastObservedAt: null,
    };
  }

  increment(key, amount = 1) {
    if (!(key in this.counters)) {
      return;
    }
    if (typeof amount === 'number') {
      this.counters[key] += amount;
      return;
    }
    this.counters[key] += amount && typeof amount.value === 'number' ? amount.value : 1;
  }

  record(key, value) {
    if (key === 'avgFeePaidXlm') {
      this.feeSamples.push(value);
      if (this.feeSamples.length > this.maxFeeSamples) {
        this.feeSamples.shift();
      }
      this.gauges.avgFeePaidXlm =
        this.feeSamples.reduce((sum, v) => sum + v, 0) /
        this.feeSamples.length;
    } else if (key === 'rpcCircuitState') {
      this.gauges.rpcCircuitState = value;
    } else if (key === 'pollFreshnessSeconds') {
      this.gauges.pollFreshnessSeconds = value;
    } else if (key === 'oldestTaskAgeSeconds') {
      this.gauges.oldestTaskAgeSeconds = value;
    } else if (key === 'retryQueueSize') {
      this.gauges.retryQueueSize = value;
    } else if (key === 'pollFreshnessSloRate') {
      this.gauges.pollFreshnessSloRate = value;
    } else if (key === 'executionTimelinessSloRate') {
      this.gauges.executionTimelinessSloRate = value;
    } else if (key in this.gauges) {
        this.feeSamples.reduce((sum, sample) => sum + sample, 0) / this.feeSamples.length;
      return;
    }
    if (key in this.gauges) {
      this.gauges[key] = value;
    }
  }

  setPollIntervalMs(ms) {
    this.pollIntervalMs = ms;
  }

  setSloThreshold(key, valueMs) {
    if (key === 'pollFreshness') {
      this.sloThresholds.pollFreshnessMs = valueMs;
    } else if (key === 'executionTimeliness') {
      this.sloThresholds.executionTimelinessMs = valueMs;
    }
  }

  getSloThreshold(key) {
    return this.sloThresholds[key] || null;
  }

  updateHealth(state) {
  updateHealth(state = {}) {
    if (state.lastPollAt) {
      this.lastPollAt = state.lastPollAt instanceof Date
        ? state.lastPollAt
        : new Date(state.lastPollAt);
    }
    if (state.lastPollCompletedAt) {
      this.lastPollCompletedAt = state.lastPollCompletedAt;
    }
    if (typeof state.rpcConnected === 'boolean') {
      this.rpcConnected = state.rpcConnected;
    }
    if (typeof state.backlogSize === 'number') {
      this.lastBacklogSize = state.backlogSize;
    }
    if (typeof state.retryBudgetPressure === 'number') {
      this.retryPressure = state.retryBudgetPressure;
    }
  }

  updateAdminState(state = {}) {
    this.adminState = {
      paused: Boolean(state.paused),
      reason: state.reason || null,
      changedAt: state.changedAt || new Date().toISOString(),
    };
  }

  updateShardState(state = {}) {
    this.shardState = { ...this.shardState, ...state };
  }

  updateDbShardState(state = {}) {
    this.dbShardState = { ...this.dbShardState, ...state };
  }

  updateDriftState(state = {}) {
    this.driftState = { ...this.driftState, ...state };
  }

  updateFailoverState(state = {}) {
    this.failoverState = {
      ...this.failoverState,
      ...state,
      endpoints: Array.isArray(state.endpoints)
        ? state.endpoints
        : this.failoverState.endpoints,
    };
  }

  snapshot() {
    return {
      ...this.counters,
      ...this.gauges,
      sloThresholds: { ...this.sloThresholds },
      admin: { ...this.adminState },
      shard: { ...this.shardState },
      dbShard: { ...this.dbShardState },
      drift: { ...this.driftState },
      failover: { ...this.failoverState },
    };
  }

  recordHistoryPoint() {
    const executed = this.counters.tasksExecutedTotal;
    const failed = this.counters.tasksFailedTotal;
    const attempts = executed + failed;
    this.history.record({
      tasksCheckedTotal: this.counters.tasksCheckedTotal,
      tasksDueTotal: this.counters.tasksDueTotal,
      tasksExecutedTotal: executed,
      tasksFailedTotal: failed,
      successRate: attempts > 0 ? executed / attempts : 1,
      avgFeePaidXlm: this.gauges.avgFeePaidXlm,
      lastCycleDurationMs: this.gauges.lastCycleDurationMs,
    });
  }

  getHealthStatus(staleThreshold) {
    const now = Date.now();
    const uptimeSeconds = Math.floor((now - this.startTime) / 1000);
    const lastPollAgeMs = this.lastPollAt ? now - this.lastPollAt.getTime() : null;
    const isStale = lastPollAgeMs == null || lastPollAgeMs > staleThreshold;
    const rpcCircuitState = this.gauges.rpcCircuitState === 2
      ? 'OPEN'
      : (this.gauges.rpcCircuitState === 1 ? 'HALF_OPEN' : 'CLOSED');
    const backlogSize = typeof this.lastBacklogSize === 'number' ? this.lastBacklogSize : 0;
    const retryBudgetPressure = this.retryPressure || 0;

    const healthIssues = [];
    if (!this.rpcConnected) {
      healthIssues.push('RPC connectivity lost');
    }
    if (rpcCircuitState === 'HALF_OPEN') {
      healthIssues.push('RPC circuit half-open');
    }
    if (rpcCircuitState === 'OPEN') {
      healthIssues.push('RPC circuit open');
    }
    if (backlogSize > 200) {
      healthIssues.push(`Polling backlog pressure: ${backlogSize} known task IDs`);
    }
    if (retryBudgetPressure >= 0.8) {
      healthIssues.push(`Retry budget pressure at ${(retryBudgetPressure * 100).toFixed(0)}%`);
    }
    if (lastPollAgeMs != null && lastPollAgeMs > staleThreshold) {
      healthIssues.push('Polling has not updated within threshold');
    }
    if (lastPollAgeMs == null) {
      healthIssues.push('No successful poll has completed yet');
    }

    let status = 'healthy';
    let statusDescription = 'Keeper is operating normally.';

    if (!this.lastPollAt || rpcCircuitState === 'OPEN') {
      status = 'unhealthy';
      statusDescription = 'Keeper is unavailable due to stale polling or broken RPC circuits.';
    } else if (isStale) {
      status = 'stale';
      statusDescription = 'Keeper polling is stale and may delay task execution. Check RPC and scheduler health.';
    } else if (backlogSize > 500 || retryBudgetPressure >= 0.95) {
      status = 'unhealthy';
      statusDescription = 'Keeper is overloaded by backlog or retry pressure and may fail to keep up with task execution.';
    } else if (!this.rpcConnected || rpcCircuitState === 'HALF_OPEN' || backlogSize > 200 || retryBudgetPressure >= 0.8) {
      status = 'degraded';
      statusDescription = 'Partial degradation detected. Some RPC, backlog, or retry behavior is impaired but the service is still responding.';
    }

    // Compute freshness: time since last completed poll
    let freshnessSeconds = 0;
    if (this.lastPollCompletedAt) {
      freshnessSeconds = Math.floor((now - this.lastPollCompletedAt.getTime()) / 1000);
    }

    return {
      status,
      statusDescription,
      statusSeverity: status === 'healthy' ? 'info' : status === 'degraded' ? 'warning' : 'critical',
      uptime: uptimeSeconds,
      lastPollAt: this.lastPollAt ? this.lastPollAt.toISOString() : null,
      lastPollCompletedAt: this.lastPollCompletedAt ? this.lastPollCompletedAt.toISOString() : null,
      pollFreshnessSeconds: freshnessSeconds,
      rpcConnected: this.rpcConnected,
      rpcCircuitState: this.gauges.rpcCircuitState === 2 ? 'OPEN' : (this.gauges.rpcCircuitState === 1 ? 'HALF_OPEN' : 'CLOSED'),
      slo: {
        pollFreshnessRate: this.gauges.pollFreshnessSloRate,
        executionTimelinessRate: this.gauges.executionTimelinessSloRate,
        thresholds: { ...this.sloThresholds },
      },
      rpcCircuitState: this.gauges.rpcCircuitState === 2
        ? 'OPEN'
        : (this.gauges.rpcCircuitState === 1 ? 'HALF_OPEN' : 'CLOSED'),
      paused: this.adminState.paused,
      pauseReason: this.adminState.reason,
      shard: { ...this.shardState },
      healthIssues,
    };
  }
}

  reset() {
    this.counters = {
      tasksCheckedTotal: 0,
      tasksDueTotal: 0,
      tasksExecutedTotal: 0,
      tasksFailedTotal: 0,
      throttledRequestsTotal: 0,
      tasksSkippedIdempotencyTotal: 0,
      pollFreshnessSloSuccess: 0,
      pollFreshnessSloFailure: 0,
      executionTimelinessSloSuccess: 0,
      executionTimelinessSloFailure: 0,
      retriesExhausted: 0,
      retriesExecutedTotal: 0,
      retriesFailedTotal: 0,
      retryAttemptsTotal: { success: 0, failure: 0, duplicate: 0 },
    };
    this.gauges = {
      avgFeePaidXlm: 0,
      lastCycleDurationMs: 0,
      rpcCircuitState: 0,
      pollFreshnessSeconds: 0,
      oldestTaskAgeSeconds: 0,
      retryQueueSize: 0,
      pollFreshnessSloRate: 0,
      executionTimelinessSloRate: 0,
    };
    this.feeSamples = [];
    tasksExecutedTotal: 0,
      tasksFailedTotal: 0,
        throttledRequestsTotal: 0,
    };
    this.gauges = {
  avgFeePaidXlm: 0,
  lastCycleDurationMs: 0,
  rpcCircuitState: 0,
};
this.feeSamples = [];
  }
function createDefaultGasMonitor() {
  return {
    getLowGasCount: () => 0,
    getConfig: () => ({
      gasWarnThreshold: 0,
      alertDebounceMs: 0,
      alertWebhookEnabled: false,
      forecastingEnabled: false,
      forecastSafetyBuffer: 0,
      forecastAggregationWindow: 0,
      dynamicFeeMultiplier: 1,
    }),
    getForecasterState: () => ({
      trackedTasks: 0,
      totalHistoricalSamples: 0,
      priceState: {
        shortTermAverage: 0,
        longTermAverage: 0,
        trend: 0,
        multiplier: 1,
      },
    }),
    getDynamicFeeMultiplier: () => 1,
  };
}

class MetricsServer {
  constructor(gasMonitor, logger, deadLetterQueue, options = {}) {
    this.gasMonitor = gasMonitor || createDefaultGasMonitor();
    this.logger = logger || createLogger('metrics');
    this.deadLetterQueue = deadLetterQueue || null;
    this.port = options.port || parseInt(process.env.METRICS_PORT, 10) || 3000;
    this.healthStaleThreshold = options.healthStaleThreshold
      || parseInt(process.env.HEALTH_STALE_THRESHOLD_MS || '60000', 10);
    this.server = null;
    this.registry = null;
    this.metrics = new Metrics();
    this.controlStateProvider = options.controlStateProvider || null;
    this.controlActionHandler = options.controlActionHandler || null;
    this.historyManager = options.historyManager || null;
    this.p2pStateProvider = options.p2pStateProvider || null;
    this.failoverStateProvider = options.failoverStateProvider || null;
    this.webhookHandler = options.webhookHandler || null;
    this.webhookPath = options.webhookPath || '/webhooks/task-executions';
    this.p2pStateProvider = options.p2pStateProvider || null;
    this.streamHub = options.streamHub || null;
    this.apiGateway = options.apiGateway || new ApiGateway({
      defaultCapacity: options.defaultGatewayCapacity,
      defaultRefillPerSecond: options.defaultGatewayRefillPerSecond,
      defaultBillingUnits: options.defaultGatewayBillingUnits,
    });
    this.failurePredictor = options.failurePredictor || new FailurePredictor({
      historyManager: this.historyManager,
      deadLetterQueue: this.deadLetterQueue,
      retryBudget: options.retryBudgetTracker || null,
      logger: createLogger('failure-predictor'),
    });
    this.reputationScorer = options.reputationScorer || new KeeperReputationScorer({
      historyManager: this.historyManager,
      logger: createLogger('reputation-scorer'),
    });
    this.register = new promClient.Registry();

    // SLO metrics — injectable for testing; shares main registry by default so
    // SLO histograms/counters appear at /metrics/prometheus without extra merging.
    this.sloMetrics = options.sloMetrics || new SloMetrics({
      register: this.register,
      freshnessTargetSeconds: options.sloFreshnessTargetSeconds,
      freshnessWarningSeconds: options.sloFreshnessWarningSeconds,
      freshnessCriticalSeconds: options.sloFreshnessCriticalSeconds,
      latenessTargetSeconds: options.sloLatenessTargetSeconds,
      latenessWarningSeconds: options.sloLatenessWarningSeconds,
      latenessCriticalSeconds: options.sloLatenessCriticalSeconds,
      sloTarget: options.sloTarget,
      errorBudgetWindowSize: options.sloErrorBudgetWindowSize,
    });

    this.initPrometheusMetrics();
  }

  setRegistry(registry) {
    this.registry = registry;
  }

   initPrometheusMetrics() {
     // Counter: Total tasks checked
     this.promTasksChecked = new promClient.Counter({
       name: 'keeper_tasks_checked_total',
       help: 'Total number of tasks checked for execution eligibility',
       registers: [this.register],
     });

     // Counter: Total tasks due for execution
     this.promTasksDue = new promClient.Counter({
       name: 'keeper_tasks_due_total',
       help: 'Total number of tasks that were due for execution',
       registers: [this.register],
     });

     // Counter: Total tasks executed successfully
     this.promTasksExecuted = new promClient.Counter({
       name: 'keeper_tasks_executed_total',
       help: 'Total number of tasks executed successfully',
       registers: [this.register],
     });

      // Counter: Total tasks failed
      this.promTasksFailed = new promClient.Counter({
        name: 'keeper_tasks_failed_total',
        help: 'Total number of tasks that failed during execution',
        registers: [this.register],
      });

      // Counter: Total tasks skipped due to idempotency lock
      this.promTasksSkippedIdempotency = new promClient.Counter({
        name: 'keeper_tasks_skipped_idempotency_total',
        help: 'Total number of tasks skipped due to idempotency lock',
        registers: [this.register],
      });

      // Counter: Total retry executions (retried tasks that succeeded)
      this.promRetriesExecuted = new promClient.Counter({
        name: 'keeper_retries_executed_total',
        help: 'Total number of retried tasks that succeeded',
        registers: [this.register],
      });

      // Counter: Total retries that failed
      this.promRetriesFailed = new promClient.Counter({
        name: 'keeper_retries_failed_total',
        help: 'Total number of retried tasks that failed',
        registers: [this.register],
      });

      // Histogram: Task execution lateness (ledger count between scheduled due and actual execution)
      this.promTaskLateness = new promClient.Histogram({
        name: 'keeper_task_execution_lateness_ledgers',
        help: 'Difference in ledger numbers between task scheduled due time and actual execution',
        buckets: [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000],
        registers: [this.register],
      });

     // Gauge: Seconds since last successful poll cycle
     this.promPollFreshnessSeconds = new promClient.Gauge({
       name: 'keeper_poll_freshness_seconds',
       help: 'Seconds since the last successful polling cycle completed',
       registers: [this.register],
     });

     // Histogram: Interval between poll cycle completions
     this.promPollInterval = new promClient.Histogram({
       name: 'keeper_poll_interval_seconds',
       help: 'Seconds between consecutive polling cycle completions',
       buckets: [1, 5, 10, 30, 60, 120, 300, 600],
       registers: [this.register],
     });

     // Gauge: Current age of oldest task in the registry (seconds since last_run)
     this.promOldestTaskAgeSeconds = new promClient.Gauge({
       name: 'keeper_oldest_task_age_seconds',
       help: 'Age of the oldest registered task (seconds since last_run)',
       registers: [this.register],
     });

     // Counter: Total requests throttled by rate limiter
     this.promThrottledRequests = new promClient.Counter({
       name: 'keeper_throttled_requests_total',
       help: 'Total number of requests throttled by the rate limiter',
       labelNames: ['limiter_name'],
       registers: [this.register],
     });

     // Counter: Total tasks skipped due to quarantine
     this.promTasksQuarantinedSkipped = new promClient.Counter({
       name: 'keeper_tasks_quarantined_skipped_total',
       help: 'Total number of tasks skipped because they are quarantined',
       registers: [this.register],
     });

     // Gauge: Number of quarantined tasks
     this.promQuarantinedCount = new promClient.Gauge({
       name: 'keeper_quarantined_tasks_count',
       help: 'Current number of tasks in quarantine',
       registers: [this.register],
     });

     // Counter: Total tasks quarantined
     this.promTotalQuarantined = new promClient.Counter({
       name: 'keeper_tasks_quarantined_total',
       help: 'Total number of tasks that have been quarantined',
       registers: [this.register],
     });

     // Counter: Total tasks recovered from quarantine
     this.promTotalRecovered = new promClient.Counter({
       name: 'keeper_tasks_recovered_total',
       help: 'Total number of tasks recovered from quarantine',
       registers: [this.register],
     });

     // Gauge: Average fee paid in XLM
     this.promAvgFee = new promClient.Gauge({
       name: 'keeper_avg_fee_paid_xlm',
       help: 'Average transaction fee paid in XLM (rolling average)',
       registers: [this.register],
     });

     // Gauge: Last cycle duration
     this.promCycleDuration = new promClient.Gauge({
       name: 'keeper_last_cycle_duration_ms',
       help: 'Duration of the last polling cycle in milliseconds',
       registers: [this.register],
     });

     // Gauge: Low gas count
     this.promLowGasCount = new promClient.Gauge({
       name: 'keeper_low_gas_count',
       help: 'Number of tasks with low gas balance',
       registers: [this.register],
     });

     // Gauge: Keeper uptime
     this.promUptime = new promClient.Gauge({
       name: 'keeper_uptime_seconds',
       help: 'Keeper service uptime in seconds since start',
       registers: [this.register],
     });

     // Gauge: RPC connection status (1 = connected, 0 = disconnected)
     this.promRpcConnected = new promClient.Gauge({
       name: 'keeper_rpc_connected',
       help: 'RPC connection status (1 = connected, 0 = disconnected)',
       registers: [this.register],
     });

     // Gauge: Forecast - underfunded tasks
     this.promUnderfundedTasks = new promClient.Gauge({
       name: 'keeper_forecast_underfunded_tasks',
       help: 'Number of tasks forecasted to be underfunded',
       registers: [this.register],
     });

     // Gauge: Forecast - high confidence forecasts
     this.promHighConfidenceForecasts = new promClient.Gauge({
       name: 'keeper_forecast_high_confidence',
       help: 'Number of tasks with high-confidence gas forecasts',
       registers: [this.register],
     });

     // Gauge: Forecast - low confidence forecasts
     this.promLowConfidenceForecasts = new promClient.Gauge({
       name: 'keeper_forecast_low_confidence',
       help: 'Number of tasks with low-confidence gas forecasts',
       registers: [this.register],
     });

     // Gauge: Forecast - risk level (0=low, 1=medium, 2=high)
     this.promForecastRiskLevel = new promClient.Gauge({
       name: 'keeper_forecast_risk_level',
       help: 'Current forecast risk level (0=low, 1=medium, 2=high)',
       registers: [this.register],
     });

     // === SLO-SPECIFIC METRICS ===

     // Histogram: Retry delay before retry attempt (seconds)
     this.promRetryDelay = new promClient.Histogram({
       name: 'keeper_retry_delay_seconds',
       help: 'Seconds waited before a retry attempt is made',
       buckets: [0.1, 0.5, 1, 2, 5, 10, 30, 60, 120],
       registers: [this.register],
     });

     // Counter: Total retry attempts with outcome label
     this.promRetryAttempts = new promClient.Counter({
       name: 'keeper_retry_attempts_total',
       help: 'Total number of retry attempts made during task execution',
       labelNames: ['outcome'], // 'success', 'failure', 'duplicate'
       registers: [this.register],
     });

     // Counter: Tasks that exceeded max retries
     this.promRetriesExhausted = new promClient.Counter({
       name: 'keeper_retries_exhausted_total',
       help: 'Total number of tasks that exhausted all retry attempts',
       registers: [this.register],
     });

     // Gauge: Current size of retry queue
     this.promRetryQueueSize = new promClient.Gauge({
       name: 'keeper_retry_queue_size',
       help: 'Current number of tasks pending retry',
       registers: [this.register],
     });

     // Histogram: Time spent in retry queue before next attempt
     this.promRetryTimeInQueue = new promClient.Histogram({
       name: 'keeper_retry_time_in_queue_seconds',
       help: 'Seconds a task spent waiting in retry queue before next attempt',
       buckets: [1, 5, 10, 30, 60, 300, 600, 1800],
       registers: [this.register],
     });

     // Counter: Tasks meeting poll freshness SLO
     this.promPollFreshnessSloSuccess = new promClient.Counter({
       name: 'keeper_poll_freshness_slo_success_total',
       help: 'Total polls that met the freshness SLO threshold',
       registers: [this.register],
     });

     // Counter: Tasks missing poll freshness SLO
     this.promPollFreshnessSloFailure = new promClient.Counter({
       name: 'keeper_poll_freshness_slo_failure_total',
       help: 'Total polls that missed the freshness SLO threshold',
       registers: [this.register],
     });

     // Counter: Tasks meeting execution timeliness SLO
     this.promExecutionTimelinessSloSuccess = new promClient.Counter({
       name: 'keeper_execution_timeliness_slo_success_total',
       help: 'Total tasks executed within the timeliness SLO threshold',
       registers: [this.register],
     });

     // Counter: Tasks missing execution timeliness SLO
     this.promExecutionTimelinessSloFailure = new promClient.Counter({
       name: 'keeper_execution_timeliness_slo_failure_total',
       help: 'Total tasks that missed the timeliness SLO threshold',
       registers: [this.register],
     });

     // Gauge: SLO success rates (computed externally, exposed as gauges for alerting)
     this.promPollFreshnessSloRate = new promClient.Gauge({
       name: 'keeper_slo_poll_freshness_rate',
       help: 'Rolling rate of poll freshness SLO success (0-1)',
       registers: [this.register],
     });

     this.promExecutionTimelinessSloRate = new promClient.Gauge({
       name: 'keeper_slo_execution_timeliness_rate',
       help: 'Rolling rate of execution timeliness SLO success (0-1)',
       registers: [this.register],
     });

     // Add default metrics (process CPU, memory, etc.)
     promClient.collectDefaultMetrics({ register: this.register });
   }
  setControlStateProvider(provider) {
    this.controlStateProvider = provider;
  }

  setWebhookHandler(handler, path = this.webhookPath) {
    this.webhookHandler = handler;
    this.webhookPath = path;
  }

  setP2PStateProvider(provider) {
    this.p2pStateProvider = provider;
  }

  setFailoverStateProvider(provider) {
    this.failoverStateProvider = provider;
  }

  initPrometheusMetrics() {
    this.promTasksChecked = new promClient.Counter({
      name: 'keeper_tasks_checked_total',
      help: 'Total number of tasks checked for execution eligibility',
      registers: [this.register],
    });
    this.promTasksDue = new promClient.Counter({
      name: 'keeper_tasks_due_total',
      help: 'Total number of tasks that were due for execution',
      registers: [this.register],
    });
    this.promTasksExecuted = new promClient.Counter({
      name: 'keeper_tasks_executed_total',
      help: 'Total number of tasks executed successfully',
      registers: [this.register],
    });
    this.promTasksFailed = new promClient.Counter({
      name: 'keeper_tasks_failed_total',
      help: 'Total number of tasks that failed during execution',
      registers: [this.register],
    });

    // Counter: Total requests throttled by rate limiter
    this.promThrottledRequests = new promClient.Counter({
      name: 'keeper_throttled_requests_total',
      help: 'Total number of requests throttled by the rate limiter',
      labelNames: ['limiter_name'],
      registers: [this.register],
    });
    this.promAdminStateChanges = new promClient.Counter({
      name: 'keeper_admin_state_changes_total',
      help: 'Total number of keeper admin state changes',
      registers: [this.register],
    });
    this.promWebhookAccepted = new promClient.Counter({
      name: 'keeper_webhook_accepted_total',
      help: 'Total inbound webhook task execution requests accepted',
      registers: [this.register],
    });
    this.promWebhookRejected = new promClient.Counter({
      name: 'keeper_webhook_rejected_total',
      help: 'Total inbound webhook task execution requests rejected',
      labelNames: ['reason'],
      registers: [this.register],
    });
    this.promWebhookReplayRejected = new promClient.Counter({
      name: 'keeper_webhook_replay_rejected_total',
      help: 'Total inbound webhook requests rejected by replay protection',
      registers: [this.register],
    });
    this.promFailoverEvents = new promClient.Counter({
      name: 'keeper_rpc_failover_events_total',
      help: 'Total RPC failover events due to endpoint failures',
      registers: [this.register],
    });
    this.promFailoverSwitches = new promClient.Counter({
      name: 'keeper_rpc_failover_switches_total',
      help: 'Total active RPC endpoint switches',
      registers: [this.register],
    });
    this.promAvgFee = new promClient.Gauge({
      name: 'keeper_avg_fee_paid_xlm',
      help: 'Average transaction fee paid in XLM (rolling average)',
      registers: [this.register],
    });
    this.promCycleDuration = new promClient.Gauge({
      name: 'keeper_last_cycle_duration_ms',
      help: 'Duration of the last polling cycle in milliseconds',
      registers: [this.register],
    });
    this.promRetryCycleDuration = new promClient.Gauge({
      name: 'keeper_last_retry_cycle_duration_ms',
      help: 'Duration of the last retry cycle in milliseconds',
      registers: [this.register],
    });
    this.promLowGasCount = new promClient.Gauge({
      name: 'keeper_low_gas_count',
      help: 'Number of tasks with low gas balance',
      registers: [this.register],
    });
    this.promSlaChecks = new promClient.Counter({
      name: 'keeper_sla_checks_total',
      help: 'Total number of SLA evaluation cycles completed',
      registers: [this.register],
    });
    this.promSlaViolations = new promClient.Counter({
      name: 'keeper_sla_violations_total',
      help: 'Total number of SLA violations detected',
      registers: [this.register],
    });
    this.promSlaSlashed = new promClient.Counter({
      name: 'keeper_sla_slashed_total',
      help: 'Total number of keeper slashing actions submitted',
      registers: [this.register],
    });
    this.promSlaLastCheckDuration = new promClient.Gauge({
      name: 'keeper_sla_last_check_duration_ms',
      help: 'Duration of the last SLA evaluation run in milliseconds',
      registers: [this.register],
    });
    this.promSlaLastSlashAmount = new promClient.Gauge({
      name: 'keeper_sla_last_slash_amount',
      help: 'Amount slashed in the most recent SLA enforcement event',
      registers: [this.register],
    });
    this.promUptime = new promClient.Gauge({
      name: 'keeper_uptime_seconds',
      help: 'Keeper service uptime in seconds',
      registers: [this.register],
    });
    this.promRpcConnected = new promClient.Gauge({
      name: 'keeper_rpc_connected',
      help: 'RPC connection status (1 = connected, 0 = disconnected)',
      registers: [this.register],
    });
    this.promRpcCircuitState = new promClient.Gauge({
      name: 'keeper_rpc_circuit_state',
      help: 'RPC circuit breaker state (0 = CLOSED, 1 = HALF_OPEN, 2 = OPEN)',
      registers: [this.register],
    });
    this.promBacklogSize = new promClient.Gauge({
      name: 'keeper_backlog_size',
      help: 'Number of task IDs currently known to the keeper registry',
      registers: [this.register],
    });
    this.promRetryBudgetPressure = new promClient.Gauge({
      name: 'keeper_retry_budget_pressure',
      help: 'Current global retry budget pressure as a fraction between 0 and 1',
      registers: [this.register],
    });
    this.promAdminPaused = new promClient.Gauge({
      name: 'keeper_admin_paused',
      help: 'Whether the keeper is administratively paused (1 = paused, 0 = active)',
      registers: [this.register],
    });
    this.promShardOwnedTasks = new promClient.Gauge({
      name: 'keeper_shard_owned_tasks',
      help: 'Number of tasks currently owned by this shard',
      labelNames: ['shard_label', 'shard_index'],
      registers: [this.register],
    });
    this.promShardSkippedTasks = new promClient.Gauge({
      name: 'keeper_shard_skipped_tasks',
      help: 'Number of tasks skipped because they are assigned to another shard',
      labelNames: ['shard_label', 'shard_index'],
      registers: [this.register],
    });
    this.promDbShardCount = new promClient.Gauge({
      name: 'keeper_db_shard_count',
      help: 'Number of Postgres database shards currently active',
      registers: [this.register],
    });
    this.promDbShardActiveUsers = new promClient.Gauge({
      name: 'keeper_db_shard_active_users',
      help: 'Active user load used for Postgres shard scaling',
      registers: [this.register],
    });
    this.promDbShardPendingTasks = new promClient.Gauge({
      name: 'keeper_db_shard_pending_tasks',
      help: 'Pending task volume used for Postgres shard scaling',
      registers: [this.register],
    });
    this.promDbShardStrategy = new promClient.Gauge({
      name: 'keeper_db_shard_strategy',
      help: 'Current Postgres shard scaling mode (0 = fixed, 1 = auto)',
      registers: [this.register],
    });
    this.promDriftSeverity = new promClient.Gauge({
      name: 'keeper_recurring_drift_severity',
      help: 'Highest currently observed recurring drift severity (0 = none, 1 = warning, 2 = critical)',
      registers: [this.register],
    });
    this.promDriftTask = new promClient.Gauge({
      name: 'keeper_recurring_drift_task_id',
      help: 'Task id associated with the highest currently observed recurring drift',
      registers: [this.register],
    });
    this.promDriftWarningCount = new promClient.Gauge({
      name: 'keeper_recurring_drift_warning_tasks',
      help: 'Number of tasks currently showing warning-level recurring drift',
      registers: [this.register],
    });
    this.promDriftCriticalCount = new promClient.Gauge({
      name: 'keeper_recurring_drift_critical_tasks',
      help: 'Number of tasks currently showing critical recurring drift',
      registers: [this.register],
    });
    this.promFailoverActiveIndex = new promClient.Gauge({
      name: 'keeper_rpc_failover_active_endpoint_index',
      help: 'Current active endpoint index for multi-region RPC failover',
      registers: [this.register],
    });
    this.promFailoverHealthyEndpoints = new promClient.Gauge({
      name: 'keeper_rpc_failover_healthy_endpoints',
      help: 'Number of healthy RPC endpoints currently available',
      registers: [this.register],
    });
    this.promFailoverTotalEndpoints = new promClient.Gauge({
      name: 'keeper_rpc_failover_total_endpoints',
      help: 'Total configured RPC endpoints for failover',
      registers: [this.register],
    });

    this.promBudgetConsumed = new promClient.Counter({
      name: 'keeper_retry_budget_consumed_total',
      help: 'Total number of retries consumed from budget',
      labelNames: ['scope'],
      registers: [this.register],
    });
    this.promBudgetExhausted = new promClient.Counter({
      name: 'keeper_retry_budget_exhausted_total',
      help: 'Total number of retry budget exhaustion events',
      labelNames: ['scope', 'reason'],
      registers: [this.register],
    });
    this.promBudgetGlobalAvailable = new promClient.Gauge({
      name: 'keeper_retry_budget_global_available',
      help: 'Global retry budget availability (0.0-1.0)',
      registers: [this.register],
    });
    this.promBudgetGlobalUsed = new promClient.Gauge({
      name: 'keeper_retry_budget_global_used',
      help: 'Global retry budget consumed',
      registers: [this.register],
    });
    this.promBudgetCooldown = new promClient.Gauge({
      name: 'keeper_retry_budget_in_cooldown',
      help: 'Whether retry budget is in cooldown (0=active, 1=cooldown)',
      registers: [this.register],
    });
    this.promBudgetCooldownRemaining = new promClient.Gauge({
      name: 'keeper_retry_budget_cooldown_remaining_ms',
      help: 'Remaining cooldown time in milliseconds',
      registers: [this.register],
    });
    this.promBudgetPressureLevel = new promClient.Gauge({
      name: 'keeper_retry_budget_pressure_level',
      help: 'Retry budget pressure level (0=low, 1=medium, 2=high, 3=critical)',
      registers: [this.register],
    });
    this.promBudgetTaskCount = new promClient.Gauge({
      name: 'keeper_retry_budget_task_count',
      help: 'Number of tasks with tracked retry budgets',
      registers: [this.register],
    });

    promClient.collectDefaultMetrics({ register: this.register });
  }

  setRetryBudgetTracker(budgetTracker) {
    this.retryBudgetTracker = budgetTracker;
  }

  syncPrometheusMetrics() {
    this.promTasksChecked.inc(0);
    this.promTasksDue.inc(0);
    this.promTasksExecuted.inc(0);
    this.promTasksFailed.inc(0);
    this.promThrottledRequests.inc({ limiter_name: 'poller-reads' }, 0);
    this.promThrottledRequests.inc({ limiter_name: 'execution-writes' }, 0);
    this.promAdminStateChanges.inc(0);
    this.promWebhookAccepted.inc(0);
    this.promWebhookRejected.inc({ reason: 'none' }, 0);
    this.promWebhookReplayRejected.inc(0);
    this.promFailoverEvents.inc(0);
    this.promFailoverSwitches.inc(0);

    // Sync tasks skipped idempotency counter
    this.promTasksSkippedIdempotency.inc(this.metrics.counters.tasksSkippedIdempotencyTotal);

    // SLO: Poll freshness counters
    this.promPollFreshnessSloSuccess.inc(this.metrics.counters.pollFreshnessSloSuccess);
    this.promPollFreshnessSloFailure.inc(this.metrics.counters.pollFreshnessSloFailure);

    // SLO: Execution timeliness counters
    this.promExecutionTimelinessSloSuccess.inc(this.metrics.counters.executionTimelinessSloSuccess);
    this.promExecutionTimelinessSloFailure.inc(this.metrics.counters.executionTimelinessSloFailure);

    // SLO: Retry metrics
    this.promRetryAttempts.inc({ outcome: 'success' }, this.metrics.counters.retryAttemptsTotal.success);
    this.promRetryAttempts.inc({ outcome: 'failure' }, this.metrics.counters.retryAttemptsTotal.failure);
    this.promRetryAttempts.inc({ outcome: 'duplicate' }, this.metrics.counters.retryAttemptsTotal.duplicate);
    this.promRetriesExhausted.inc(this.metrics.counters.retriesExhausted);
    this.promRetriesExecuted.inc(this.metrics.counters.retriesExecutedTotal);
    this.promRetriesFailed.inc(this.metrics.counters.retriesFailedTotal);

    // SLO gauges
    this.promPollFreshnessSeconds.set(this.metrics.gauges.pollFreshnessSeconds);
    this.promOldestTaskAgeSeconds.set(this.metrics.gauges.oldestTaskAgeSeconds);
    this.promRetryQueueSize.set(this.metrics.gauges.retryQueueSize);
    this.promPollFreshnessSloRate.set(this.metrics.gauges.pollFreshnessSloRate);
    this.promExecutionTimelinessSloRate.set(this.metrics.gauges.executionTimelinessSloRate);

    this.promAvgFee.set(this.metrics.gauges.avgFeePaidXlm);
    this.promCycleDuration.set(this.metrics.gauges.lastCycleDurationMs);
    this.promRetryCycleDuration.set(this.metrics.gauges.lastRetryCycleDurationMs);
    this.promLowGasCount.set(this.gasMonitor.getLowGasCount());
    this.promSlaChecks.inc(0);
    this.promSlaViolations.inc(0);
    this.promSlaSlashed.inc(0);
    this.promSlaLastCheckDuration.set(this.metrics.gauges.slaLastCheckDurationMs);
    this.promSlaLastSlashAmount.set(this.metrics.gauges.slaLastSlashAmount);
    this.promUptime.set(Math.floor((Date.now() - this.metrics.startTime) / 1000));
    this.promRpcConnected.set(this.metrics.rpcConnected ? 1 : 0);
    this.promRpcCircuitState.set(this.metrics.gauges.rpcCircuitState);
    this.promBacklogSize.set(this.metrics.lastBacklogSize || 0);
    this.promRetryBudgetPressure.set(this.metrics.retryPressure || 0);
    this.promAdminPaused.set(this.metrics.adminState.paused ? 1 : 0);
    this.promShardOwnedTasks.set(
      {
        shard_label: String(this.metrics.shardState.shardLabel),
        shard_index: String(this.metrics.shardState.shardIndex),
      },
      this.metrics.shardState.ownedTasks,
    );
    this.promShardSkippedTasks.set(
      {
        shard_label: String(this.metrics.shardState.shardLabel),
        shard_index: String(this.metrics.shardState.shardIndex),
      },
      this.metrics.shardState.skippedTasks,
    );
    this.promDriftSeverity.set(
      this.metrics.driftState.severity === 'critical'
        ? 2
        : (this.metrics.driftState.severity === 'warning' ? 1 : 0),
    );
    this.promDriftTask.set(this.metrics.driftState.taskId || 0);
    this.promDriftWarningCount.set(this.metrics.driftState.warning || 0);
    this.promDriftCriticalCount.set(this.metrics.driftState.critical || 0);
    this.promDbShardCount.set(this.metrics.dbShardState.dbShardCount);
    this.promDbShardActiveUsers.set(this.metrics.dbShardState.activeUsers);
    this.promDbShardPendingTasks.set(this.metrics.dbShardState.pendingTasks);
    this.promDbShardStrategy.set(this.metrics.dbShardState.dbShardStrategy === 'auto' ? 1 : 0);

    if (typeof this.failoverStateProvider === 'function') {
      try {
        this.metrics.updateFailoverState(this.failoverStateProvider());
      } catch (error) {
        this.logger.error('Error reading failover state', { error: error.message });
      }
    }
    this.promFailoverActiveIndex.set(this.metrics.failoverState.activeIndex || 0);
    this.promFailoverHealthyEndpoints.set(this.metrics.failoverState.healthyEndpoints || 0);
    this.promFailoverTotalEndpoints.set(this.metrics.failoverState.totalEndpoints || 0);

    if (this.retryBudgetTracker) {
      const budgetStats = this.retryBudgetTracker.getStats();
      this.promBudgetGlobalAvailable.set(budgetStats.global.available);
      this.promBudgetGlobalUsed.set(budgetStats.global.used);
      this.promBudgetCooldown.set(budgetStats.cooldownActive ? 1 : 0);
      this.promBudgetCooldownRemaining.set(budgetStats.cooldownRemainingMs);
      this.promBudgetTaskCount.set(budgetStats.taskCount);

      const pressureMap = { low: 0, medium: 1, high: 2, critical: 3 };
      this.promBudgetPressureLevel.set(pressureMap[budgetStats.pressure] || 0);
    }
  }

  incrementBudgetConsumed(scope = 'global') {
    if (this.promBudgetConsumed) {
      this.promBudgetConsumed.inc({ scope });
    }
  }

    // Compute poll freshness dynamically based on last completion time
    const freshnessSeconds = this.metrics.lastPollCompletedAt
      ? Math.floor((Date.now() - this.metrics.lastPollCompletedAt.getTime()) / 1000)
      : 0;
    this.promPollFreshnessSeconds.set(freshnessSeconds);
  incrementBudgetExhausted(scope = 'global', reason = 'limit') {
    if (this.promBudgetExhausted) {
      this.promBudgetExhausted.inc({ scope, reason });
    }
  }

  start() {
    if (this.server) {
      return;
    }

    this.server = http.createServer(async (req, res) => {
      const protect = (handler) => {
        return () => requireAdminAuth(req, res, handler);
      };

      res.setHeader('Access-Control-Allow-Origin', '*');
      res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
      res.setHeader('Access-Control-Allow-Headers', 'Content-Type');

      if (req.method === 'OPTIONS') {
        res.writeHead(204);
        res.end();
        return;
      }

      const url = new URL(req.url, `http://127.0.0.1:${this.port}`);
      const routePath = url.pathname;

      if (this.apiGateway && routePath !== '/health' && routePath !== '/health/') {
        const gatewayDecision = this.apiGateway.evaluate(req, routePath);
        if (!gatewayDecision.allowed) {
          this.increment('throttledRequestsTotal', { name: 'api-gateway' });
          res.writeHead(429, {
            'Content-Type': 'application/json',
            'Retry-After': Math.ceil((gatewayDecision.retryAfterMs || 1000) / 1000),
          });
          res.end(JSON.stringify({
            error: 'Too Many Requests',
            route: routePath,
            retryAfterMs: gatewayDecision.retryAfterMs,
            policy: gatewayDecision.policy,
          }, null, 2));
          return;
        }
      }

      if (url.pathname === '/health' || url.pathname === '/health/') {
        this.handleHealth(res);

      } else if (req.url === '/metrics' || req.url === '/metrics/') {
        this.handleMetrics(res);

      } else if (req.url === '/metrics/prometheus' || req.url === '/metrics/prometheus/') {
        this.handlePrometheusMetrics(res);

      } else if (req.url === '/metrics/forecast' || req.url === '/metrics/forecast/') {
        this.handleForecast(res);

      } else if (req.url === '/metrics/failure-risk' || req.url === '/metrics/failure-risk/') {
        this.handleFailureRisk(res);

      } else if (req.url === '/metrics/reputation' || req.url === '/metrics/reputation/') {
        this.handleReputation(res);

      } else if (req.url === '/metrics/slo' || req.url === '/metrics/slo/') {
        this.handleSloMetrics(res);

      } else if (url.pathname === '/metrics/history' || url.pathname === '/metrics/history/') {
        this.handleMetricsHistory(req, res);

      } else if (req.url === '/admin/billing' || req.url === '/admin/billing/') {
        protect(() => this.handleBilling(res))();


        // 🔐 PROTECTED ROUTES START HERE

      } else if (req.url === '/admin/reset' && req.method === 'POST') {
        protect(() => {
          this.metrics.reset();
          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ success: true }));
        })();

      } else if (req.url === '/admin/dead-letter') {
        protect(() => this.handleDeadLetter(res))();

      } else if (req.url.startsWith('/admin/dead-letter/')) {
        protect(() => this.handleDeadLetterTask(req, res))();

      } else if (req.url === '/admin/fraud' || req.url === '/admin/fraud/') {
        protect(() => this.handleFraudState(res))();

      } else if (req.url === '/admin/reconciliation' || req.url === '/admin/reconciliation/') {
        protect(() => this.handleReconciliationState(res))();

      } else if (url.pathname === this.webhookPath && this.webhookHandler) {
        // Webhook requests (unauthenticated - auth handled by webhook handler)
        this.webhookHandler.handle(req, res);

        // ❌ NOT FOUND
      } else if (url.pathname === '/drift' || url.pathname === '/drift/') {
        this.handleDrift(res);
      } else if (url.pathname === '/admin/keeper' || url.pathname === '/admin/keeper/') {
        this.handleAdminState(req, res);
      } else if (url.pathname === '/admin/keeper/pause' || url.pathname === '/admin/keeper/pause/') {
        await this.handlePauseResume(req, res, true);
      } else if (url.pathname === '/admin/keeper/resume' || url.pathname === '/admin/keeper/resume/') {
        await this.handlePauseResume(req, res, false);
      } else if (url.pathname === '/admin/fraud' || url.pathname === '/admin/fraud/') {
        this.handleFraudState(res);
      } else if (url.pathname === '/admin/reconciliation' || url.pathname === '/admin/reconciliation/') {
        this.handleReconciliationState(res);
      } else {
        res.writeHead(404);
        res.end('Not Found');
      }
    });

    this.server.listen(this.port, () => {
      this.logger.info(`Metrics server running on port ${this.port}`);
      if (this.streamHub && typeof this.streamHub.start === 'function') {
        this.streamHub.start(this.server).catch((error) => {
          this.logger.error('Failed to start realtime stream hub', { error: error.message });
        });
      }
    });

    this.server.listen(this.port, () => {
      this.logger.info(`Server running on port ${this.port}`);
      this.logger.info(`WebSocket enabled on http://localhost:${this.port}`);
    });
  }

   broadcast(event, data) {
     if (this.io) {
       this.io.emit(event, data);
     }
   }

   // === SLO Metrics Recording Methods ===

    /**
     * Record poll cycle completion for freshness tracking.
     * Call after a poll cycle finishes.
     * @param {number} pollCycleDurationMs - Duration of the poll cycle in ms
     * @param {number} intervalMs - Expected polling interval in ms (used for first cycle)
     */
    recordPollCycle(pollCycleDurationMs, intervalMs = this.metrics.pollIntervalMs) {
      const now = Date.now();
      const previousCompletion = this.metrics.lastPollCompletedAt ? this.metrics.lastPollCompletedAt.getTime() : null;
      this.metrics.lastPollCompletedAt = new Date(now);

      // Determine actual interval between poll completions
      let actualIntervalMs = pollCycleDurationMs;
      if (previousCompletion) {
        actualIntervalMs = now - previousCompletion;
      } else {
        // First cycle: use configured interval as approximation
        actualIntervalMs = intervalMs;
      }

      // Record poll interval histogram (seconds)
      this.promPollInterval.observe(actualIntervalMs / 1000);

      // SLO: Check if poll met freshness threshold (i.e., completed within expected interval)
      if (actualIntervalMs <= this.metrics.sloThresholds.pollFreshnessMs) {
        this.metrics.increment('pollFreshnessSloSuccess');
      } else {
        this.metrics.increment('pollFreshnessSloFailure');
      }

      this._computeSloRates();
      this.broadcast('sync:metrics', this.metrics.snapshot());
    }

    /**
     * Record task execution for timeliness SLO.
     * Call when a task execution completes (success or failure).
     * @param {number} taskId - The task ID
     * @param {number} actualExecutionLedger - Ledger when execution completed
     * @param {number} scheduledDueLedger - Ledger when task was due
     * @param {boolean} success - Whether execution succeeded
     */
    recordTaskExecution({ taskId, actualExecutionLedger, scheduledDueLedger, success }) {
      const latenessLedgers = Math.max(0, actualExecutionLedger - scheduledDueLedger);
      this.promTaskLateness.observe(latenessLedgers);

      // Convert to milliseconds assuming ~5s per ledger on testnet (configurable)
      const avgLedgerTimeMs = parseInt(process.env.LEDGER_TIME_MS || '5000', 10);
      const latenessMs = latenessLedgers * avgLedgerTimeMs;

      // SLO: Check if execution met timeliness threshold
      if (latenessMs <= this.metrics.sloThresholds.executionTimelinessMs) {
        this.metrics.increment('executionTimelinessSloSuccess');
      } else {
        this.metrics.increment('executionTimelinessSloFailure');
      }

      this._computeSloRates();
      this.broadcast('sync:metrics', this.metrics.snapshot());
    }

   /**
    * Record retry attempt.
    * @param {string} outcome - 'success', 'failure', or 'duplicate'
    */
   recordRetryAttempt(outcome) {
     this.metrics.increment('retryAttemptsTotal', { outcome });
   }

   /**
    * Record retry scheduling event.
    * @param {number} delayMs - Delay before retry in milliseconds
    */
   recordRetryDelay(delayMs) {
     this.promRetryDelay.observe(delayMs / 1000);
   }

   /**
    * Record task time spent in retry queue before next attempt.
    * @param {number} timeInQueueMs - Time spent waiting in milliseconds
    */
   recordRetryTimeInQueue(timeInQueueMs) {
     this.promRetryTimeInQueue.observe(timeInQueueMs / 1000);
   }

   /**
    * Update retry queue size gauge.
    * @param {number} size - Current retry queue size
    */
   setRetryQueueSize(size) {
     this.metrics.record('retryQueueSize', size);
   }

   /**
    * Update oldest task age (time since last_run).
    * @param {number} oldestAgeSeconds - Age in seconds of the oldest task
    */
   setOldestTaskAge(oldestAgeSeconds) {
     this.metrics.record('oldestTaskAgeSeconds', oldestAgeSeconds);
   }

   /**
    * Compute rolling SLO success rates.
    * Called internally after each SLO observation.
    */
   _computeSloRates() {
     const totalPoll = this.metrics.counters.pollFreshnessSloSuccess + this.metrics.counters.pollFreshnessSloFailure;
     if (totalPoll > 0) {
       this.metrics.record('pollFreshnessSloRate', this.metrics.counters.pollFreshnessSloSuccess / totalPoll);
     }

     const totalExec = this.metrics.counters.executionTimelinessSloSuccess + this.metrics.counters.executionTimelinessSloFailure;
     if (totalExec > 0) {
       this.metrics.record('executionTimelinessSloRate', this.metrics.counters.executionTimelinessSloSuccess / totalExec);
     }
   }
  }

  handleHealth(res) {
    const status = this.metrics.getHealthStatus(this.healthStaleThreshold);
    const healthData = {
      ...status,
      p2p: this.getP2PState(),
      failover: this.getFailoverState(),
      ...(this.retryBudgetTracker && {
        retryBudget: this.retryBudgetTracker.getStats(),
      }),
    };
    res.writeHead(['stale', 'unhealthy'].includes(status.status) ? 503 : 200, {
      'Content-Type': 'application/json',
    });
    res.end(JSON.stringify(healthData, null, 2));
  }

  handleMetricsHistory(req, res) {
    const url = new URL(req.url || '/metrics/history', 'http://localhost');
    const limit = Math.min(
      parseInt(url.searchParams.get('limit') || '60', 10),
      this.metrics.history.maxSamples,
    );
    const samples = this.metrics.history.getSamples(limit);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ samples, count: samples.length }, null, 2));
  }

  handleMetrics(res) {
    const gasConfig = this.gasMonitor.getConfig();
    const forecasterState = this.gasMonitor.getForecasterState();
    const metricsData = {
      ...this.metrics.snapshot(),
      lowGasCount: this.gasMonitor.getLowGasCount(),
      gasWarnThreshold: gasConfig.gasWarnThreshold,
      alertDebounceMs: gasConfig.alertDebounceMs,
      alertWebhookEnabled: gasConfig.alertWebhookEnabled,
      forecasting: {
        enabled: gasConfig.forecastingEnabled,
        safetyBuffer: gasConfig.forecastSafetyBuffer,
        aggregationWindowSeconds: gasConfig.forecastAggregationWindow,
        trackedTasks: forecasterState.trackedTasks,
        totalHistoricalSamples: forecasterState.totalHistoricalSamples,
      },
      p2p: this.getP2PState(),
      failover: this.getFailoverState(),
      ...(this.retryBudgetTracker && {
        retryBudget: this.retryBudgetTracker.getStats(),
      }),
    };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(metricsData, null, 2));
  }

  getP2PState() {
    if (typeof this.p2pStateProvider !== 'function') {
      return { enabled: false };
    }
    try {
      return this.p2pStateProvider();
    } catch (error) {
      this.logger.error('Error reading P2P state', { error: error.message });
      return { enabled: true, status: 'error' };
    }
  }

  getFailoverState() {
    if (typeof this.failoverStateProvider === 'function') {
      try {
        const next = this.failoverStateProvider();
        this.metrics.updateFailoverState(next);
      } catch (error) {
        this.logger.error('Error reading failover state', { error: error.message });
      }
    }
    return this.metrics.failoverState;
  }

  handleForecast(res) {
    const forecastData = this.gasMonitor.getForecasterState();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(forecastData, null, 2));
  }

  handleFailureRisk(res) {
    const tasks = this.historyManager?.getRecentExecutions
      ? this.historyManager.getRecentExecutions(25)
      : [];
    const taskIds = [...new Set(tasks.map((entry) => entry.taskId).filter((taskId) => taskId != null))];
    const predictions = this.failurePredictor?.predictBatch
      ? this.failurePredictor.predictBatch(taskIds)
      : { predictions: [], highestRisk: null, averageRiskScore: 0 };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ...predictions,
      sampleCount: tasks.length,
    }, null, 2));
  }

  handleReputation(res) {
    const queueStatus = this.retryBudgetTracker?.getStats
      ? this.retryBudgetTracker.getStats()
      : null;
    const score = this.reputationScorer?.scoreKeeper
      ? this.reputationScorer.scoreKeeper({
        uptimeSeconds: Math.floor((Date.now() - this.metrics.startTime) / 1000),
        expectedUptimeSeconds: Math.max(1, Math.floor((Date.now() - this.metrics.startTime) / 1000)),
        completedTasks: this.metrics.counters.tasksExecutedTotal,
        expectedTasks: Math.max(1, this.metrics.counters.tasksExecutedTotal + this.metrics.counters.tasksFailedTotal),
        stakeAmount: queueStatus?.global?.used || 0,
        maxStakeAmount: Math.max(1, queueStatus?.global?.limit || 1),
        missedHeartbeats: this.metrics.driftState.critical || 0,
      })
      : { reputationScore: 0, reputationTier: 'low', signals: {}, evidence: {} };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ...score,
      admin: { ...this.metrics.adminState },
    }, null, 2));
  }

  handleBilling(res) {
    const usage = this.apiGateway?.getUsageSummary ? this.apiGateway.getUsageSummary() : {
      totalRequests: 0,
      totalThrottled: 0,
      totalBilledUnits: 0,
      routes: {},
    };

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      ...usage,
      pricing: {
        billingUnit: 1,
        currency: 'request-units',
      },
    }, null, 2));
  }

  /**
   * GET /metrics/slo — SLO snapshot in JSON.
   *
   * Returns poll freshness status, error budget consumption, lateness SLI data,
   * configured SLO targets, and documented known measurement limitations.
   * This endpoint is unauthenticated (read-only, no sensitive data).
   */
  handleSloMetrics(res) {
    const snapshot = this.sloMetrics.getSnapshot();
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(snapshot, null, 2));
  }

  handleDrift(res) {
    const payload = {
      summary: this.metrics.driftState,
      tasks: this.historyManager?.getDriftSnapshot
        ? this.historyManager.getDriftSnapshot()
        : [],
    };
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(payload, null, 2));
  }

  handleFraudState(res) {
    if (!this.fraudDetector) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Fraud detection unavailable' }));
      return;
    }

    try {
      const payload = this.fraudDetector.getState();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload, null, 2));
    } catch (error) {
      this.logger.error('Error reading fraud detection state', { error: error.message });
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Failed to read fraud state' }));
    }
  }

  handleReconciliationState(res) {
    if (!this.reconciliationEngine) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Reconciliation unavailable' }));
      return;
    }

    try {
      const payload = this.reconciliationEngine.getState();
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(payload, null, 2));
    } catch (error) {
      this.logger.error('Error reading reconciliation state', { error: error.message });
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Failed to read reconciliation state' }));
    }
  }

  async handlePrometheusMetrics(res) {
    try {
      this.syncPrometheusMetrics();
      const metrics = await this.register.metrics();
      res.writeHead(200, { 'Content-Type': this.register.contentType });
      res.end(metrics);
    } catch (error) {
      this.logger.error('Error generating Prometheus metrics', { error: error.message });
      res.writeHead(500, { 'Content-Type': 'text/plain' });
      res.end('Internal Server Error');
    }
  }

  isAuthorized(req) {
    const configuredToken = process.env.KEEPER_ADMIN_TOKEN;
    if (!configuredToken) {
      return false;
    }
    const authHeader = req.headers.authorization || '';
    const token = authHeader.replace(/^Bearer\s+/i, '');
    return token === configuredToken;
  }

  handleAdminState(req, res) {
    if (!this.isAuthorized(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }

    const state = this.controlStateProvider ? this.controlStateProvider() : this.metrics.adminState;
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state, null, 2));
  }

  async handlePauseResume(req, res, paused) {
    if (req.method !== 'POST') {
      res.writeHead(405, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Method Not Allowed' }));
      return;
    }
    if (!this.isAuthorized(req)) {
      res.writeHead(401, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Unauthorized' }));
      return;
    }
    if (typeof this.controlActionHandler !== 'function') {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Admin controls unavailable' }));
      return;
    }

    const body = await this.readJsonBody(req);
    const state = await this.controlActionHandler({
      paused,
      reason: body.reason || null,
      actor: body.actor || 'api',
    });

    this.metrics.updateAdminState(state);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(state, null, 2));
  }

  readJsonBody(req) {
    return new Promise((resolve) => {
      let raw = '';
      req.on('data', (chunk) => {
        raw += chunk;
      });
      req.on('end', () => {
        if (!raw) {
          resolve({});
          return;
        }
        try {
          resolve(JSON.parse(raw));
        } catch (_) {
          resolve({});
        }
      });
    });
  }

  /**
   * GET /reconcile — return the most recent reconciliation report, or a
   * 204 No Content when no reconciliation has run yet.
   */
  handleReconcileStatus(res) {
    if (!this.reconciler) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Reconciler not initialised' }));
      return;
    }

    const report = this.reconciler.getLastReport();
    if (!report) {
      res.writeHead(204);
      res.end();
      return;
    }

    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify(report, null, 2));
  }

  /**
   * POST /reconcile — trigger an immediate reconciliation pass.
   * Returns 409 Conflict when one is already running.
   * Returns 200 with the report on success.
   */
  handleReconcileTrigger(res) {
    if (!this.reconciler) {
      res.writeHead(503, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Reconciler not initialised' }));
      return;
    }

    this.reconciler.reconcile().then((report) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(report, null, 2));
    }).catch((err) => {
      const status = err.code === 'RECONCILIATION_IN_PROGRESS' ? 409 : 500;
      res.writeHead(status, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: err.message, code: err.code ?? null }));
    });
  }

  updateHealth(state) {
    this.metrics.updateHealth(state);
  }

  increment(key, amount) {
    this.metrics.increment(key, amount);
    if (key === 'tasksCheckedTotal') {
      this.promTasksChecked.inc(amount);
    } else if (key === 'tasksDueTotal') {
      this.promTasksDue.inc(amount);
    } else if (key === 'tasksExecutedTotal') {
      this.promTasksExecuted.inc(amount);
    } else if (key === 'tasksFailedTotal') {
      this.promTasksFailed.inc(amount);
    } else if (key === 'tasksSkippedIdempotencyTotal') {
      this.promTasksSkippedIdempotency.inc(amount);
    } else if (key === 'throttledRequestsTotal') {
      this.promThrottledRequests.inc({ limiter_name: amount.name || 'unknown' }, amount.value || 1);
    } else if (key === 'pollFreshnessSloSuccess') {
      this.promPollFreshnessSloSuccess.inc(amount);
    } else if (key === 'pollFreshnessSloFailure') {
      this.promPollFreshnessSloFailure.inc(amount);
    } else if (key === 'executionTimelinessSloSuccess') {
      this.promExecutionTimelinessSloSuccess.inc(amount);
    } else if (key === 'executionTimelinessSloFailure') {
      this.promExecutionTimelinessSloFailure.inc(amount);
    } else if (key === 'retriesExhausted') {
      this.promRetriesExhausted.inc(amount);
    } else if (key === 'retriesExecutedTotal') {
      this.promRetriesExecuted.inc(amount);
    } else if (key === 'retriesFailedTotal') {
      this.promRetriesFailed.inc(amount);
    } else if (key === 'retryAttemptsTotal' && typeof amount === 'object') {
      const outcome = amount.outcome || 'unknown';
      this.promRetryAttempts.inc({ outcome }, 1);
      this.promThrottledRequests.inc(
        { limiter_name: amount?.name || 'unknown' },
        amount?.value || 1,
      );
    } else if (key === 'adminStateChangesTotal') {
      this.promAdminStateChanges.inc(typeof amount === 'number' ? amount : 1);
    } else if (key === 'failoverEventsTotal') {
      this.promFailoverEvents.inc(typeof amount === 'number' ? amount : 1);
    } else if (key === 'failoverSwitchesTotal') {
      this.promFailoverSwitches.inc(typeof amount === 'number' ? amount : 1);
    }
  }

  record(key, value) {
    this.metrics.record(key, value);
    if (key === 'avgFeePaidXlm') {
      this.promAvgFee.set(this.metrics.gauges.avgFeePaidXlm);
    } else if (key === 'lastCycleDurationMs') {
      this.promCycleDuration.set(value);
    } else if (key === 'pollFreshnessSeconds') {
      this.promPollFreshnessSeconds.set(value);
    } else if (key === 'oldestTaskAgeSeconds') {
      this.promOldestTaskAgeSeconds.set(value);
    } else if (key === 'retryQueueSize') {
      this.promRetryQueueSize.set(value);
    } else if (key === 'pollFreshnessSloRate') {
      this.promPollFreshnessSloRate.set(value);
    } else if (key === 'executionTimelinessSloRate') {
      this.promExecutionTimelinessSloRate.set(value);
    } else if (key === 'lastRetryCycleDurationMs') {
      this.promRetryCycleDuration.set(value);
    } else if (key === 'rpcCircuitState') {
      this.promRpcCircuitState.set(value);
    } else if (key === 'fraudRiskScore') {
      this.promFraudRiskScore.set(value);
    }
  }

  updateShardState(state) {
    this.metrics.updateShardState(state);
  }

  updateDbShardState(state) {
    this.metrics.updateDbShardState(state);
  }

  updateDriftState(state) {
    this.metrics.updateDriftState(state);
  }

  updateFraudState(state) {
    this.metrics.updateFraudState(state);
    this.promFraudRiskScore.set(this.metrics.fraudState.lastRiskScore || 0);
    this.promFraudPendingAlerts.set(this.metrics.fraudState.pendingAlerts || 0);
  }

  updateReconciliationState(state) {
    this.metrics.updateReconciliationState(state);
    this.promReconciliationDrift.set(this.metrics.reconciliationState.lastDrift || 0);
    this.promReconciliationPending.set(this.metrics.reconciliationState.pendingExecutions || 0);
  }

  updateAdminState(state) {
    this.metrics.updateAdminState(state);
  }

  updateFailoverState(state) {
    this.metrics.updateFailoverState(state);
  }

  stop() {
    if (this.server) {
      this.server.close();
      this.server = null;
      this.logger.info('Server stopped');
    }
  }
}

module.exports = { Metrics, MetricsHistory, MetricsServer };
