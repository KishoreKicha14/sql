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
      if (threadContext.getHeader(QueryInsightsMarker.PARENT_HEADER) == null) {
        String value =
            QueryInsightsMarker.value(
                "PPL", clusterServiceRef.localNode().getId(), pplQueryTask.getId());
        threadContext.putHeader(QueryInsightsMarker.PARENT_HEADER, value);
      }
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
  private void registerQueryInsightsReport(PPLQueryTask reportTask) {
    try {
      boolean registered =
          reportTask.addResourceTrackingCompletionListener(
              new org.opensearch.core.action.NotifyOnceListener<>() {
                @Override
                protected void innerOnResponse(org.opensearch.tasks.Task task) {
                  sendQueryInsightsReport((PPLQueryTask) task);
                }

                @Override
                protected void innerOnFailure(Exception e) {
                  // Resource tracking failed to complete cleanly; nothing to report.
                }
              });
      if (!registered) {
        LOG.debug("PPL task resource tracking already complete; skipping Query Insights report");
      }
    } catch (Exception e) {
      LOG.warn("Failed to register Query Insights report listener", e);
    }
  }

  /**
   * Build and best-effort send the {@link ReportQueryRequest} for a completed PPL query. Reads the
   * finalized coordinator resource stats, the stashed query profile, and the query text. Any
   * failure (including Query Insights not being installed) is swallowed.
   */
  private void sendQueryInsightsReport(PPLQueryTask reportTask) {
    try {
      String coordinatorId = clusterServiceRef.localNode().getId() + ":" + reportTask.getId();
      String queryText = stripPplPrefix(reportTask.getDescription());

      org.opensearch.core.tasks.resourcetracker.TaskResourceUsage usage =
          reportTask.getTotalResourceStats();
      long cpuNanos = usage == null ? 0L : usage.getCpuTimeInNanos();
      long memoryBytes = usage == null ? 0L : usage.getMemoryInBytes();

      QueryProfile profile = reportTask.getQueryProfile();
      long latencyMillis = 0L;
      java.util.List<ReportQueryRequest.PhaseMetric> phases = new java.util.ArrayList<>();
      if (profile != null) {
        if (profile.getSummary() != null) {
          latencyMillis = Math.round(profile.getSummary().getTotalTimeMillis());
        }
        if (profile.getPhases() != null) {
          profile
              .getPhases()
              .forEach(
                  (name, phase) ->
                      phases.add(
                          new ReportQueryRequest.PhaseMetric(
                              name,
                              phase.getTimeMillis(),
                              phase.getCpuTimeMillis(),
                              phase.getMemoryBytes())));
        }
      }

      ReportQueryRequest request =
          new ReportQueryRequest(
              "PPL",
              coordinatorId,
              queryText,
              null,
              latencyMillis,
              cpuNanos,
              memoryBytes,
              System.currentTimeMillis(),
              phases);

      clientRef.execute(ReportQueryAction.INSTANCE, request, ActionListener.wrap(r -> {}, e -> {}));
    } catch (Exception e) {
      // Query Insights may not be installed, or the send may fail; never affect query execution.
      LOG.debug("Failed to report PPL query to Query Insights", e);
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
      stampQueryInsightsParentHeader(reportTask);
      registerQueryInsightsReport(reportTask);
    }
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_TOTAL).increment();
    Metrics.getInstance().getNumericalMetric(MetricName.PPL_REQ_COUNT_TOTAL).increment();

    QueryContext.addRequestId();

    // in order to use PPL service, we need to convert TransportPPLQueryRequest to PPLQueryRequest
    PPLQueryRequest transformedRequest = transportRequest.toPPLQueryRequest();
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
   * Snapshot the per-phase profile onto the coordinator task (so the resource-tracking completion
   * listener can report it to Query Insights), then clear the profiling thread-local. Runs on the
   * execution thread where the profile is still bound. Best-effort: never throws into the response
   * path.
   */
  private void stashProfileThenClear(PPLQueryTask reportTask) {
    try {
      if (reportTask != null) {
        reportTask.setQueryProfile(QueryProfiling.current().finish());
      }
    } catch (Exception e) {
      LOG.warn("Failed to capture PPL query profile for Query Insights", e);
    } finally {
      QueryProfiling.clear();
    }
  }
}
