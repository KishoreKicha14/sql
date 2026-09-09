/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import com.sun.management.ThreadMXBean;
import java.lang.management.ManagementFactory;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.opensearch.OpenSearchTimeoutException;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.tasks.resourcetracker.ResourceStats;
import org.opensearch.core.tasks.resourcetracker.ResourceStatsType;
import org.opensearch.core.tasks.resourcetracker.ResourceUsageMetric;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryId;
import org.opensearch.sql.executor.QueryManager;
import org.opensearch.sql.executor.execution.AbstractPlan;
import org.opensearch.sql.monitor.profile.ProfileCapturingTask;
import org.opensearch.sql.monitor.profile.QueryProfiling;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

/** QueryManager implemented in OpenSearch cluster. */
@RequiredArgsConstructor
public class OpenSearchQueryManager implements QueryManager {

  private static final Logger LOG = LogManager.getLogger(OpenSearchQueryManager.class);

  /**
   * Used to sample per-thread CPU and allocated-memory usage for coordinator-task resource
   * tracking. Resolved to the {@code com.sun.management} bean when available (the same one
   * OpenSearch core uses); left null otherwise so tracking degrades to a no-op.
   */
  private static final ThreadMXBean THREAD_MX_BEAN = resolveThreadMXBean();

  private static ThreadMXBean resolveThreadMXBean() {
    try {
      java.lang.management.ThreadMXBean bean = ManagementFactory.getThreadMXBean();
      if (bean instanceof ThreadMXBean) {
        return (ThreadMXBean) bean;
      }
    } catch (Exception e) {
      LOG.warn("Per-thread resource metrics unavailable; PPL task resource tracking disabled", e);
    }
    return null;
  }

  private final NodeClient nodeClient;

  private final Settings settings;

  public static final String SQL_WORKER_THREAD_POOL_NAME = "sql-worker";
  public static final String SQL_COMPLEX_WORKER_THREAD_POOL_NAME = "sql-complex-worker";
  public static final String SQL_BACKGROUND_THREAD_POOL_NAME = "sql_background_io";

  private static final ThreadLocal<CancellableTask> cancellableTask = new ThreadLocal<>();

  public static void setCancellableTask(CancellableTask task) {
    cancellableTask.set(task);
  }

  public static CancellableTask getCancellableTask() {
    return cancellableTask.get();
  }

  public static void clearCancellableTask() {
    cancellableTask.remove();
  }

  /**
   * Carries the Query Insights parent marker ({@code PPL:<nodeId>:<taskId>}) across the engine's
   * thread hops, alongside {@link #cancellableTask}. Set by the SQL/PPL coordinator on the calling
   * thread, re-established on the sql-worker pool by {@link #schedule}, and read by
   * {@code BackgroundSearchScanner} so it can stamp the marker onto each child DSL search.
   *
   * <p>The engine hops from the coordinator thread to the sql-worker pool (which rebinds only the
   * Log4j ThreadContext, not OpenSearch's header-carrying ThreadContext) and again to the
   * sql_background_io pool. Reading the header off the OpenSearch ThreadContext at each hop is
   * racy — Calcite may evaluate join sides / prefetch batches on threads that never received it. A
   * dedicated ThreadLocal, propagated exactly like {@link #cancellableTask} (which reliably reaches
   * the background pool today), makes child tagging deterministic.
   */
  private static final ThreadLocal<String> queryInsightsParentMarker = new ThreadLocal<>();

  public static void setQueryInsightsParentMarker(String marker) {
    queryInsightsParentMarker.set(marker);
  }

  public static String getQueryInsightsParentMarker() {
    return queryInsightsParentMarker.get();
  }

  public static void clearQueryInsightsParentMarker() {
    queryInsightsParentMarker.remove();
  }

  @Override
  public QueryId submit(AbstractPlan queryPlan) {
    TimeValue timeout = settings.getSettingValue(Settings.Key.PPL_QUERY_TIMEOUT);
    CancellableTask cancelTask = cancellableTask.get();
    cancellableTask.remove();
    // Capture the Query Insights parent marker on the coordinator thread (set there alongside the
    // cancellable task) and carry it to the worker thread the same way, so child DSL searches this
    // query spawns — including per-side join scans — are tagged deterministically.
    String parentMarker = queryInsightsParentMarker.get();
    queryInsightsParentMarker.remove();
    schedule(nodeClient, queryPlan::execute, timeout, cancelTask, parentMarker);

    return queryPlan.getQueryId();
  }

  private void schedule(
      NodeClient client,
      Runnable task,
      TimeValue timeout,
      CancellableTask cancelTask,
      String parentMarker) {
    ThreadPool threadPool = client.threadPool();

    Runnable wrappedTask =
        withCurrentContext(
            () -> {
              final Thread executionThread = Thread.currentThread();
              // Re-establish the parent marker on this worker thread so downstream scans can read it
              // (mirrors setCancellableTask below).
              setQueryInsightsParentMarker(parentMarker);

              Scheduler.ScheduledCancellable timeoutTask =
                  threadPool.schedule(
                      () -> {
                        LOG.warn(
                            "Query execution timed out after {}. Interrupting execution thread.",
                            timeout);
                        executionThread.interrupt();
                      },
                      timeout,
                      ThreadPool.Names.GENERIC);

              setCancellableTask(cancelTask);

              // Attribute this worker thread's CPU/memory to the coordinator task. The engine
              // executes on the sql-worker pool, and the worker hop rebinds only the Log4j
              // ThreadContext (not OpenSearch's TASK_ID transient), so OpenSearch's automatic
              // TaskAwareRunnable association does not fire here. We therefore bracket execution
              // ourselves using the same public Task API and ThreadMXBean metrics that core's
              // TaskResourceTrackingService uses. No-op unless the task opts into resource
              // tracking.
              final boolean trackResources =
                  cancelTask != null && cancelTask.supportsResourceTracking();
              final long trackedThreadId = Thread.currentThread().getId();
              if (trackResources) {
                startThreadResourceTracking(cancelTask, trackedThreadId);
              }

              try {
                task.run();
                timeoutTask.cancel();
                // Clear any leftover thread interrupts to keep the thread pool clean
                Thread.interrupted();
              } catch (Exception e) {
                timeoutTask.cancel();

                // Special-case handling of timeout-related interruptions
                if (Thread.interrupted() || e.getCause() instanceof InterruptedException) {
                  LOG.error("Query was interrupted due to timeout after {}", timeout);
                  throw new OpenSearchTimeoutException(
                      "Query execution timed out after " + timeout);
                }

                throw e;
              } finally {
                if (trackResources) {
                  stopThreadResourceTracking(cancelTask, trackedThreadId);
                }
                // Capture the per-phase profile snapshot HERE, on the sql-worker execution thread,
                // where the profiling ThreadLocal is still bound. The transport completion listener
                // runs on a different thread where QueryProfiling.current() is the no-op context, so
                // capturing there loses the phase breakdown. Stash it onto the task (if it opts in)
                // for the completion listener to report to Query Insights. Best-effort.
                if (cancelTask instanceof ProfileCapturingTask) {
                  try {
                    ((ProfileCapturingTask) cancelTask).setQueryProfile(QueryProfiling.current().finish());
                  } catch (Exception e) {
                    LOG.debug("Failed to capture query profile for Query Insights", e);
                  }
                }
                clearCancellableTask();
                clearQueryInsightsParentMarker();
              }
            });

    threadPool.schedule(wrappedTask, new TimeValue(0), SQL_WORKER_THREAD_POOL_NAME);
  }

  private Runnable withCurrentContext(final Runnable task) {
    final Map<String, String> currentContext = ThreadContext.getImmutableContext();
    return () -> {
      ThreadContext.putAll(currentContext);
      task.run();
    };
  }

  /**
   * Record the starting CPU/memory snapshot for {@code threadId} against {@code task}, mirroring
   * {@code TaskResourceTrackingService}. Any failure is swallowed so resource accounting can never
   * break query execution.
   */
  private static void startThreadResourceTracking(CancellableTask task, long threadId) {
    try {
      task.startThreadResourceTracking(
          threadId, ResourceStatsType.WORKER_STATS, currentThreadResourceMetrics(threadId));
    } catch (Exception e) {
      LOG.warn("Failed to start resource tracking for task [{}]", task.getId(), e);
    }
  }

  /**
   * Record the final CPU/memory snapshot for {@code threadId} against {@code task}. Any failure is
   * swallowed so resource accounting can never break query execution.
   */
  private static void stopThreadResourceTracking(CancellableTask task, long threadId) {
    try {
      task.stopThreadResourceTracking(
          threadId, ResourceStatsType.WORKER_STATS, currentThreadResourceMetrics(threadId));
    } catch (Exception e) {
      LOG.warn("Failed to stop resource tracking for task [{}]", task.getId(), e);
    }
  }

  /**
   * Current per-thread memory and CPU usage, matching what OpenSearch core records for its own
   * resource-aware tasks. Returns an empty array if the JVM does not expose the metric so tracking
   * degrades gracefully instead of throwing.
   */
  private static ResourceUsageMetric[] currentThreadResourceMetrics(long threadId) {
    if (THREAD_MX_BEAN == null) {
      return new ResourceUsageMetric[0];
    }
    ResourceUsageMetric memory =
        new ResourceUsageMetric(
            ResourceStats.MEMORY, THREAD_MX_BEAN.getThreadAllocatedBytes(threadId));
    ResourceUsageMetric cpu =
        new ResourceUsageMetric(ResourceStats.CPU, THREAD_MX_BEAN.getThreadCpuTime(threadId));
    return new ResourceUsageMetric[] {memory, cpu};
  }
}
