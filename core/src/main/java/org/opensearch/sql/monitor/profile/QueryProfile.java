/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import com.google.gson.annotations.SerializedName;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import lombok.Getter;

/** Immutable snapshot of query profiling metrics. */
@Getter
public final class QueryProfile {

  private final Summary summary;

  private final Map<String, Phase> phases;

  /** Execution-engine-specific plan profile: a {@link PlanNode} tree, or a pre-rendered object. */
  private final Object plan;

  @SerializedName("thread_pool")
  private final String threadPool;

  /**
   * Create a new query profile snapshot.
   *
   * @param totalTimeMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param phases per-phase measurements keyed by {@link MetricName}
   */
  public QueryProfile(double totalTimeMillis, Map<MetricName, PhaseMeasurement> phases) {
    this(totalTimeMillis, phases, null, null);
  }

  /**
   * Create a new query profile snapshot.
   *
   * @param totalTimeMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param phases per-phase measurements keyed by {@link MetricName}
   * @param plan plan tree profiling output
   */
  public QueryProfile(
      double totalTimeMillis, Map<MetricName, PhaseMeasurement> phases, Object plan) {
    this(totalTimeMillis, phases, plan, null);
  }

  /**
   * Create a new query profile snapshot.
   *
   * @param totalTimeMillis total elapsed milliseconds for the query (rounded to two decimals)
   * @param phases per-phase measurements keyed by {@link MetricName}
   * @param plan plan tree profiling output
   * @param threadPool thread pool name that executed the query
   */
  public QueryProfile(
      double totalTimeMillis,
      Map<MetricName, PhaseMeasurement> phases,
      Object plan,
      String threadPool) {
    Objects.requireNonNull(phases, "phases");
    this.phases = buildPhases(phases);
    this.summary = buildSummary(totalTimeMillis, this.phases.values());
    this.plan = plan;
    this.threadPool = threadPool;
  }

  private Map<String, Phase> buildPhases(Map<MetricName, PhaseMeasurement> phases) {
    Map<String, Phase> ordered = new LinkedHashMap<>(MetricName.values().length);
    for (MetricName metricName : MetricName.values()) {
      PhaseMeasurement m = phases.getOrDefault(metricName, PhaseMeasurement.ZERO);
      ordered.put(
          metricName.name().toLowerCase(Locale.ROOT),
          new Phase(m.timeMillis(), m.cpuTimeMillis(), m.memoryBytes()));
    }
    return ordered;
  }

  private Summary buildSummary(double totalTimeMillis, Iterable<Phase> phases) {
    double totalCpu = 0d;
    long totalMemory = 0L;
    for (Phase phase : phases) {
      totalCpu += phase.getCpuTimeMillis();
      totalMemory += phase.getMemoryBytes();
    }
    return new Summary(totalTimeMillis, totalCpu, totalMemory);
  }

  /**
   * Immutable per-phase measurement passed in when building a profile: elapsed time, CPU time (both
   * in milliseconds) and allocated memory in bytes.
   */
  public record PhaseMeasurement(double timeMillis, double cpuTimeMillis, long memoryBytes) {
    public static final PhaseMeasurement ZERO = new PhaseMeasurement(0d, 0d, 0L);
  }

  @Getter
  public static final class Summary {

    @SerializedName("total_time_ms")
    private final double totalTimeMillis;

    @SerializedName("total_cpu_time_ms")
    private final double totalCpuTimeMillis;

    @SerializedName("total_memory_bytes")
    private final long totalMemoryBytes;

    private Summary(double totalTimeMillis, double totalCpuTimeMillis, long totalMemoryBytes) {
      this.totalTimeMillis = totalTimeMillis;
      this.totalCpuTimeMillis = totalCpuTimeMillis;
      this.totalMemoryBytes = totalMemoryBytes;
    }
  }

  @Getter
  public static final class Phase {

    @SerializedName("time_ms")
    private final double timeMillis;

    @SerializedName("cpu_time_ms")
    private final double cpuTimeMillis;

    @SerializedName("memory_bytes")
    private final long memoryBytes;

    private Phase(double timeMillis, double cpuTimeMillis, long memoryBytes) {
      this.timeMillis = timeMillis;
      this.cpuTimeMillis = cpuTimeMillis;
      this.memoryBytes = memoryBytes;
    }
  }

  @Getter
  public static final class PlanNode {

    private final String node;

    @SerializedName("time_ms")
    private final double timeMillis;

    private final long rows;

    private final List<PlanNode> children;

    public PlanNode(String node, double timeMillis, long rows, List<PlanNode> children) {
      this.node = node;
      this.timeMillis = timeMillis;
      this.rows = rows;
      this.children = children;
    }
  }
}
