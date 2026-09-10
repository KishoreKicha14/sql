/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.opensearch.rest.BaseRestHandler.MULTI_ALLOW_EXPLICIT_INDEX;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse.normalizeLf;
import static org.opensearch.sql.lang.PPLLangSpec.PPL_SPEC;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.calcite.rel.RelNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Guice;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.inject.ModulesBuilder;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.common.utils.QueryContext;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.datasources.service.DataSourceServiceImpl;
import org.opensearch.sql.executor.AnalyzeResponse;
import org.opensearch.sql.executor.ExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.monitor.profile.ProfileScope;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.executor.tracing.TracingPhaseListener;
import org.opensearch.sql.opensearch.setting.OpenSearchSettings;
import org.opensearch.sql.plugin.config.EngineExtensionsHolder;
import org.opensearch.sql.plugin.config.OpenSearchPluginModule;
import org.opensearch.sql.plugin.rest.AnalyticsEngineFormatSupport;
import org.opensearch.sql.plugin.rest.AnalyticsExecutorHolder;
import org.opensearch.sql.plugin.rest.RestUnifiedQueryAction;
import org.opensearch.sql.ppl.PPLService;
import org.opensearch.sql.ppl.domain.PPLQueryRequest;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.CsvResponseFormatter;
import org.opensearch.sql.protocol.response.format.Format;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.RawResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.sql.protocol.response.format.SimpleJsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.VisualizationResponseFormatter;
import org.opensearch.sql.protocol.response.format.YamlResponseFormatter;
import org.opensearch.tasks.Task;
import org.opensearch.telemetry.tracing.Span;
import org.opensearch.telemetry.tracing.SpanCreationContext;
import org.opensearch.telemetry.tracing.SpanScope;
import org.opensearch.telemetry.tracing.Tracer;
import org.opensearch.telemetry.tracing.attributes.Attributes;
import org.opensearch.telemetry.tracing.listener.TraceableActionListener;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

/** Send PPL query transport action. */
public class TransportPPLQueryAction
    extends HandledTransportAction<ActionRequest, TransportPPLQueryResponse> {

  private static final Logger LOG = LogManager.getLogger(TransportPPLQueryAction.class);

  private final Injector injector;

  private final Tracer tracer;

  private final Supplier<Boolean> pplEnabled;

  /** Null when analytics-engine plugin is absent; set via {@link #setQueryPlanExecutor}. */
  private volatile RestUnifiedQueryAction unifiedQueryHandler;

  private final NodeClient clientRef;
  private final ClusterService clusterServiceRef;
  private final org.opensearch.sql.common.setting.Settings pluginSettingsRef;
  private final TransportService transportServiceRef;

  @Inject
  public TransportPPLQueryAction(
      TransportService transportService,
      ActionFilters actionFilters,
      NodeClient client,
      ClusterService clusterService,
      DataSourceServiceImpl dataSourceService,
      org.opensearch.common.settings.Settings clusterSettings,
      EngineExtensionsHolder extensionsHolder,
      Tracer tracer) {
    super(PPLQueryAction.NAME, transportService, actionFilters, TransportPPLQueryRequest::new);
    this.clientRef = client;
    this.clusterServiceRef = clusterService;
    this.transportServiceRef = transportService;

    ModulesBuilder modules = new ModulesBuilder();
    modules.add(new OpenSearchPluginModule(extensionsHolder.engines(), tracer));
    org.opensearch.sql.common.setting.Settings pluginSettings =
        new OpenSearchSettings(clusterService.getClusterSettings());
    this.pluginSettingsRef = pluginSettings;
    modules.add(
        b -> {
          b.bind(NodeClient.class).toInstance(client);
          b.bind(org.opensearch.sql.common.setting.Settings.class).toInstance(pluginSettings);
          b.bind(DataSourceService.class).toInstance(dataSourceService);
        });
    this.injector = Guice.createInjector(modules);
    this.tracer = tracer;
    ProfileScope.installListener(new TracingPhaseListener(tracer));
    this.pplEnabled =
        () ->
            MULTI_ALLOW_EXPLICIT_INDEX.get(clusterSettings)
                && (Boolean)
                    injector
                        .getInstance(org.opensearch.sql.common.setting.Settings.class)
                        .getSettingValue(Settings.Key.PPL_ENABLED);
  }

  /** Invoked by Guice iff analytics-engine bound {@code QueryPlanExecutor}. */
  @Inject(optional = true)
  public void setQueryPlanExecutor(
      QueryPlanExecutor<RelNode, Iterable<Object[]>> queryPlanExecutor) {
    AnalyticsExecutorHolder.set(queryPlanExecutor);
    // Build the SQL router once both bridges are populated (engine context might arrive
    // first or last depending on Guice ordering). buildUnifiedQueryHandler is idempotent.
    buildUnifiedQueryHandlerIfReady();
  }

  /** Invoked by Guice iff analytics-engine bound {@code EngineContextProvider}. */
  @Inject(optional = true)
  public void setEngineContext(org.opensearch.analytics.EngineContextProvider contextProvider) {
    org.opensearch.sql.plugin.rest.EngineContextProviderHolder.set(contextProvider);
    buildUnifiedQueryHandlerIfReady();
  }

  private void buildUnifiedQueryHandlerIfReady() {
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = AnalyticsExecutorHolder.get();
    org.opensearch.analytics.EngineContextProvider contextProvider =
        org.opensearch.sql.plugin.rest.EngineContextProviderHolder.get();
    if (executor != null && contextProvider != null) {
      this.unifiedQueryHandler =
          new RestUnifiedQueryAction(
              clientRef,
              clusterServiceRef,
              executor,
              contextProvider,
              pluginSettingsRef,
              new org.opensearch.sql.opensearch.executor.ThreadPoolExecutionDispatcher(
                  clientRef.threadPool(), pluginSettingsRef));
    }
  }

  /**
   * Stamp the Query Insights parent marker ({@code PPL:<nodeId>:<taskId>}) into the thread context
   * so it rides on every child DSL search this query issues. Registered via {@code
   * SQLPlugin.getTaskHeaders()}, the header is copied into the child search tasks (including on
   * remote data nodes), letting Query Insights classify each child and associate it back to this
   * PPL query. Best-effort: any failure is swallowed so header stamping never breaks query
   * execution, and it does not overwrite a header already present (e.g. a nested PPL call).
   */
  private void stampQueryInsightsParentHeader(PPLQueryTask pplQueryTask) {
    try {
      org.opensearch.common.util.concurrent.ThreadContext threadContext =
          clientRef.threadPool().getThreadContext();
      String value =
          QueryInsightsMarker.value(
              "PPL", clusterServiceRef.localNode().getId(), pplQueryTask.getId());
      if (threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER) == null) {
        threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, value);
      }
      // Also carry the marker on the engine's ThreadLocal channel (alongside the cancellable task),
      // so child DSL searches spawned on the background scan pool are tagged deterministically. The
      // OpenSearch ThreadContext header alone does not survive the engine's thread hops (sql-worker
      // -> sql_background_io), which caused intermittent child capture for join scans.
      org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.setQueryInsightsParentMarker(
          value);
    } catch (Exception e) {
      LOG.warn("Failed to stamp Query Insights parent header for query association", e);
    }
  }

  /**
   * Register a completion listener on the coordinator task that reports the finished PPL query to
   * Query Insights. The listener fires exactly once when the task's last resource-tracked thread
   * closes (across inline, complex-worker, and background-prefetch paths), so {@code
   * getTotalResourceStats()} is finalized when it runs. Best-effort: if tracking is already
   * complete (listener not accepted) or the report fails, query execution is unaffected.
   */
  /**
   * Query Insights owns the operator-facing toggle for recording PPL queries into Top N. It is a
   * dynamic cluster setting registered by the Query Insights plugin under this key; the two plugins
   * do not share classes, so the key string is the contract between them. Since this plugin does not
   * register the setting, its value is read directly from cluster state (transient overrides
   * persistent). Default false when unset or Query Insights is not installed. Read fresh per query so
   * the toggle takes effect at runtime. Defensive: any read failure defaults to disabled.
   */
  static final String QUERY_INSIGHTS_PPL_ENABLED_KEY = "search.insights.top_queries.ppl.enabled";

  /**
   * Records a completed PPL query into Query Insights only when BOTH hold:
   *
   * <ol>
   *   <li><b>Query Insights is installed.</b> Detected by whether the setting key above is
   *       registered in this node's {@link org.opensearch.common.settings.ClusterSettings}. That
   *       setting is registered by the Query Insights plugin, so its presence is an exact,
   *       zero-cost, local signal that Query Insights is on the node. If Query Insights is absent
   *       the key is unregistered (and cannot even be set), so this returns false and we never write
   *       to an index no one reads.
   *   <li><b>Recording is enabled.</b> The dynamic setting value is {@code true} (default false),
   *       read fresh from cluster state so the operator toggle takes effect at runtime.
   * </ol>
   *
   * Defensive: any failure defaults to disabled so reporting never affects query execution.
   */
  private boolean isQueryInsightsRecordingEnabled() {
    try {
      // (1) Query Insights installed? The setting is registered iff Query Insights is present.
      if (clusterServiceRef.getClusterSettings().get(QUERY_INSIGHTS_PPL_ENABLED_KEY) == null) {
        return false;
      }
      // (2) Recording enabled? Transient overrides persistent; absent → false.
      org.opensearch.common.settings.Settings persistent =
          clusterServiceRef.state().metadata().persistentSettings();
      org.opensearch.common.settings.Settings transientSettings =
          clusterServiceRef.state().metadata().transientSettings();
      boolean fromPersistent = persistent.getAsBoolean(QUERY_INSIGHTS_PPL_ENABLED_KEY, false);
      return transientSettings.getAsBoolean(QUERY_INSIGHTS_PPL_ENABLED_KEY, fromPersistent);
    } catch (Exception e) {
      LOG.debug("Failed to evaluate Query Insights PPL recording gate; defaulting to disabled", e);
      return false;
    }
  }

  private void registerQueryInsightsReport(PPLQueryTask reportTask) {
    try {
      boolean registered =
          reportTask.addResourceTrackingCompletionListener(
              new org.opensearch.core.action.NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(org.opensearch.tasks.Task task) {
                  writeQueryInsightsRecord((PPLQueryTask) task);
                }

                @Override
                protected void innerOnFailure(Exception e) {
                  // Resource tracking failed to complete cleanly; nothing to report.
                }
              });
      if (registered == false) {
        LOG.debug(
            "Query Insights report listener not registered; resource tracking already complete");
      }
    } catch (Exception e) {
      LOG.warn("Failed to register Query Insights report listener", e);
    }
  }

  /**
   * Best-effort: report a completed PPL query to Query Insights so it appears in the Query Insights
   * in-memory Top N (and, on window rotation, the historical index) with its child DSL searches
   * rolled up. Reads the finalized coordinator resource stats, the stashed query profile, and the
   * query text, then serializes them and sends them to Query Insights over the transport layer as a
   * core {@link org.opensearch.transport.BytesTransportRequest} (see {@link QueryInsightsReporter}
   * for why a transport send of a core request type). Any failure — including Query Insights not
   * being installed, so the action is unregistered — is swallowed and never affects query
   * execution.
   */
  private void writeQueryInsightsRecord(PPLQueryTask reportTask) {
    try {
      String nodeId = clusterServiceRef.localNode().getId();
      String queryText = stripPplPrefix(reportTask.getDescription());

      org.opensearch.core.tasks.resourcetracker.TaskResourceUsage usage =
          reportTask.getTotalResourceStats();
      long cpuNanos = usage == null ? 0L : usage.getCpuTimeInNanos();
      long memoryBytes = usage == null ? 0L : usage.getMemoryInBytes();

      // End-to-end wall-clock latency measured at the coordinator: the task's start time (set at
      // registration, i.e. query start) to now (this listener fires when the query has finished).
      // This is used instead of the query profile's total time, because the profile total is
      // computed from a thread-local profiling context and collapses to ~0 when finish() resolves
      // on a different thread than the one activated at query start. getStartTimeNanos() is a
      // monotonic clock, so the delta is a reliable elapsed duration.
      long latencyMillis = Math.max(0L, (System.nanoTime() - reportTask.getStartTimeNanos()) / 1_000_000L);

      QueryProfile profile = reportTask.getQueryProfile();
      Map<String, Map<String, Object>> phases = new java.util.HashMap<>();
      if (profile != null) {
        if (profile.getPhases() != null) {
          profile
              .getPhases()
              .forEach(
                  (name, phase) -> {
                    Map<String, Object> p = new java.util.HashMap<>();
                    p.put("time_ms", phase.getTimeMillis());
                    p.put("cpu_time_ms", phase.getCpuTimeMillis());
                    p.put("memory_bytes", phase.getMemoryBytes());
                    phases.put(name, p);
                  });
        }
      }

      // The parent marker MUST equal the value stamped into the child DSL task header
      // (QueryInsightsMarker.value), so Query Insights can roll child CPU/memory into this parent:
      // child DERIVED_FROM == this parent's marker. It is the qualified "<source>:<nodeId>:<taskId>",
      // not the bare "<nodeId>:<taskId>" coordinatorId.
      String parentMarker = QueryInsightsMarker.value("PPL", nodeId, reportTask.getId());

      // Shape hash for SIMILARITY grouping is intentionally empty for now: PPL/SQL similarity
      // grouping is deferred (the implementation lives on feat/ppl-query-insights-grouping). The
      // field is still sent to keep the wire format aligned with the Query Insights handler.
      String queryShapeHash = "";

      // Hand the record to Query Insights over the transport layer (core BytesTransportRequest), so
      // it flows through the in-memory addRecord pipeline: in-memory Top N + historical + roll-up.
      QueryInsightsReporter.report(
          transportServiceRef,
          clusterServiceRef.localNode(),
          "PPL",
          parentMarker,
          nodeId,
          queryText,
          System.currentTimeMillis(),
          latencyMillis,
          cpuNanos,
          memoryBytes,
          phases,
          queryShapeHash);
    } catch (Exception e) {
      // Query Insights may not be installed, or the write may fail; never affect query execution.
      LOG.debug("Failed to write PPL query to Query Insights", e);
    }
  }

  /** Strip the {@code "PPL: "} / {@code "PPL [queryId=...]: "} description prefix. */
  private static String stripPplPrefix(String description) {
    if (description == null) {
      return "";
    }
    int colon = description.indexOf(": ");
    if (description.startsWith("PPL") && colon >= 0) {
      return description.substring(colon + 2);
    }
    return description;
  }

  /**
   * {@inheritDoc} Transform the request and call super.doExecute() to support call from other
   * plugins.
   */
  @Override
  protected void doExecute(
      Task task, ActionRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    if (!pplEnabled.get()) {
      listener.onFailure(
          new IllegalAccessException(
              "Either plugins.ppl.enabled or rest.action.multi.allow_explicit_index setting is"
                  + " false"));
      return;
    }

    TransportPPLQueryRequest transportRequest = TransportPPLQueryRequest.fromActionRequest(request);
    if (transportRequest.isGrammarRequest()) {
      // Authorization is enforced by this transport action before returning grammar metadata in
      // REST.
      listener.onResponse(new TransportPPLQueryResponse("{}"));
      return;
    }

    final PPLQueryTask reportTask = task instanceof PPLQueryTask ? (PPLQueryTask) task : null;
    if (reportTask != null) {
      OpenSearchQueryManager.setCancellableTask(reportTask);
      // Recording PPL queries into Query Insights (Top N) is opt-in via a dynamic cluster setting.
      // When disabled, we neither stamp the parent header (so child DSL searches are not tagged)
      // nor register the report writer — zero Query Insights side effects.
      if (isQueryInsightsRecordingEnabled()) {
        stampQueryInsightsParentHeader(reportTask);
        registerQueryInsightsReport(reportTask);
      }
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
    // Only the request's own profile flag controls the expensive per-operator plan-node profiling.
    // Query Insights does NOT need to force it: per-phase time/CPU/memory is captured by the cheap
    // always-on phase metrics in QueryService, and query-total CPU/memory comes from the task
    // resource-tracking framework — all without plan-node instrumentation.
    QueryContext.setProfile(transformedRequest.profile());

    // Start root span with OTel DB semantic convention attributes
    Span rootSpan =
        tracer.startSpan(
            SpanCreationContext.client()
                .name("opensearch.query")
                .attributes(
                    Attributes.create()
                        .addAttribute("db.system.name", "opensearch")
                        .addAttribute("db.query.type", "ppl")
                        .addAttribute("db.query.id", QueryContext.getRequestId())
                        .addAttribute(
                            "db.operation.name",
                            transformedRequest.isExplainRequest() ? "EXPLAIN" : "EXECUTE")));

    // Put span in scope so ThreadContext propagation captures it
    SpanScope spanScope = tracer.withSpanInScope(rootSpan);

    // Trace wrapper: ends span in async callback, sets error on failure.
    ActionListener<TransportPPLQueryResponse> tracedListener =
        TraceableActionListener.create(listener, rootSpan, tracer);
    ActionListener<TransportPPLQueryResponse> clearingListener =
        wrapWithProfilingClear(tracedListener, reportTask);

    try {
      // Route to analytics engine for non-Lucene (e.g., Parquet-backed) indices.
      if (unifiedQueryHandler != null
          && unifiedQueryHandler.isAnalyticsIndex(transformedRequest.getRequest(), QueryType.PPL)) {
        LOG.info("[{}] Routing PPL query to analytics engine", QueryContext.getRequestId());
        // Pass this PPL task so the analytics engine links its query task to it for cancellation.
        if (transformedRequest.isExplainRequest()) {
          unifiedQueryHandler.explain(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.mode(),
              task,
              createExplainResponseListener(transformedRequest, clearingListener));
        } else {
          // Analytics route only emits JSON; reject unsupported formats (e.g. csv) with a 4xx.
          try {
            AnalyticsEngineFormatSupport.validateFormat(format(transformedRequest));
          } catch (Exception e) {
            clearingListener.onFailure(e);
            return;
          }
          unifiedQueryHandler.execute(
              transformedRequest.getRequest(),
              QueryType.PPL,
              transformedRequest.profile(),
              transformedRequest.getFetchSize(),
              task,
              clearingListener);
        }
        return;
      }

      Consumer<String> anonymizedQuerySink =
          anonymized -> rootSpan.addAttribute("db.query.text", anonymized);
      PPLService pplService = injector.getInstance(PPLService.class);
      if (transformedRequest.isExplainRequest()) {
        pplService.explain(
            transformedRequest,
            createExplainResponseListener(transformedRequest, clearingListener),
            anonymizedQuerySink);
      } else if (transformedRequest.analyze()) {
        pplService.analyze(
            transformedRequest,
            createAnalyzeResponseListener(transformedRequest, clearingListener),
            anonymizedQuerySink);
      } else {
        pplService.execute(
            transformedRequest,
            createListener(transformedRequest, clearingListener),
            createExplainResponseListener(transformedRequest, clearingListener),
            anonymizedQuerySink);
      }
    } catch (Exception e) {
      clearingListener.onFailure(e);
    } finally {
      spanScope.close();
    }
  }

  private ResponseListener<AnalyzeResponse> createAnalyzeResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<AnalyzeResponse>() {
      @Override
      public void onResponse(AnalyzeResponse response) {
        JsonResponseFormatter<AnalyzeResponse> formatter =
            new JsonResponseFormatter<>(PRETTY) {
              @Override
              protected Object buildJsonObject(AnalyzeResponse response) {
                return response;
              }
            };
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  /**
   * TODO: need to extract an interface for both SQL and PPL action handler and move these common
   * methods to the interface. This is not easy to do now because SQL action handler is still in
   * legacy module.
   */
  private ResponseListener<ExecutionEngine.ExplainResponse> createExplainResponseListener(
      PPLQueryRequest request, ActionListener<TransportPPLQueryResponse> listener) {
    return new ResponseListener<ExecutionEngine.ExplainResponse>() {
      @Override
      public void onResponse(ExecutionEngine.ExplainResponse response) {
        Optional<Format> isYamlFormat =
            Format.ofExplain(request.getFormat()).filter(format -> format.equals(Format.YAML));
        ResponseFormatter<ExecutionEngine.ExplainResponse> formatter;
        if (isYamlFormat.isPresent()) {
          formatter =
              new YamlResponseFormatter<>() {
                @Override
                protected Object buildYamlObject(ExecutionEngine.ExplainResponse response) {
                  return normalizeLf(response);
                }
              };
        } else {
          formatter =
              new JsonResponseFormatter<>(PRETTY) {
                @Override
                protected Object buildJsonObject(ExecutionEngine.ExplainResponse response) {
                  // For json_tree format, use parsed tree objects instead of strings
                  if (response.getCalcite() != null
                      && response.getCalcite().getLogicalTree() != null) {
                    Map<String, Object> result = new LinkedHashMap<>();
                    Map<String, Object> calcite = new LinkedHashMap<>();
                    calcite.put("logical", response.getCalcite().getLogicalTree());
                    if (response.getCalcite().getPhysicalTree() != null) {
                      calcite.put("physical", response.getCalcite().getPhysicalTree());
                    }
                    result.put("calcite", calcite);
                    return result;
                  }
                  return response;
                }
              };
        }
        listener.onResponse(
            new TransportPPLQueryResponse(formatter.format(response), formatter.contentType()));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private ResponseListener<ExecutionEngine.QueryResponse> createListener(
      PPLQueryRequest pplRequest, ActionListener<TransportPPLQueryResponse> listener) {
    Format format = format(pplRequest);
    ResponseFormatter<QueryResult> formatter;
    if (format.equals(Format.CSV)) {
      formatter = new CsvResponseFormatter(pplRequest.sanitize());
    } else if (format.equals(Format.RAW)) {
      formatter = new RawResponseFormatter();
    } else if (format.equals(Format.VIZ)) {
      formatter = new VisualizationResponseFormatter(pplRequest.style());
    } else {
      formatter = new SimpleJsonResponseFormatter(JsonResponseFormatter.Style.PRETTY);
    }

    return new ResponseListener<ExecutionEngine.QueryResponse>() {
      @Override
      public void onResponse(ExecutionEngine.QueryResponse response) {
        String responseContent =
            formatter.format(
                new QueryResult(
                    response.getSchema(), response.getResults(), response.getCursor(), PPL_SPEC));
        listener.onResponse(new TransportPPLQueryResponse(responseContent));
      }

      @Override
      public void onFailure(Exception e) {
        listener.onFailure(e);
      }
    };
  }

  private Format format(PPLQueryRequest pplRequest) {
    String format = pplRequest.getFormat();
    Optional<Format> optionalFormat = Format.of(format);
    if (optionalFormat.isPresent()) {
      return optionalFormat.get();
    } else {
      throw new IllegalArgumentException(
          String.format(Locale.ROOT, "response in %s format is not supported.", format));
    }
  }

  private ActionListener<TransportPPLQueryResponse> wrapWithProfilingClear(
      ActionListener<TransportPPLQueryResponse> delegate, PPLQueryTask reportTask) {
    return new ActionListener<>() {
      @Override
      public void onResponse(TransportPPLQueryResponse transportPPLQueryResponse) {
        try {
          delegate.onResponse(transportPPLQueryResponse);
        } finally {
          stashProfileThenClear(reportTask);
        }
      }

      @Override
      public void onFailure(Exception e) {
        try {
          delegate.onFailure(e);
        } finally {
          stashProfileThenClear(reportTask);
        }
      }
    };
  }

  /**
   * Clear any profiling thread-local bound to the current thread.
   *
   * <p>The per-phase profile is now captured on the {@code sql-worker} execution thread (in
   * {@code OpenSearchQueryManager}, where the profiling context is actually bound) and stashed onto
   * the task via {@link org.opensearch.sql.monitor.profile.ProfileCapturingTask}. This method must
   * NOT re-stash from here: it runs on the response-completion thread, where
   * {@code QueryProfiling.current()} is the no-op context, and stashing it would overwrite the good
   * capture with an empty profile. It only clears, as a defensive cleanup. Best-effort.
   */
  private void stashProfileThenClear(PPLQueryTask reportTask) {
    try {
      QueryProfiling.clear();
    } catch (Exception e) {
      LOG.debug("Failed to clear PPL query profiling context", e);
    }
  }
}
