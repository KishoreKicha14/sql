/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tests that per-phase profiling accumulates CPU time and allocated memory alongside wall-clock
 * time, and that the totals roll up into the {@link QueryProfile} summary.
 */
class PhaseResourceProfilingTest {

  @AfterEach
  void clearProfiling() {
    QueryProfiling.clear();
  }

  @Test
  void defaultMetricAccumulatesTimeCpuAndMemory() {
    DefaultMetricImpl metric = new DefaultMetricImpl("EXECUTE");
    metric.record(100, 40, 512);
    metric.record(50, 10, 128);

    assertEquals(150, metric.value(), "time is the sum of recorded elapsed values");
    assertEquals(50, metric.cpuNanos());
    assertEquals(640, metric.memoryBytes());
  }

  @Test
  void defaultMetricIgnoresNonPositiveCpuAndMemory() {
    DefaultMetricImpl metric = new DefaultMetricImpl("ANALYZE");
    // Time is always accumulated; unsupported/negative cpu and memory samples are dropped.
    metric.record(100, -1, -1);
    metric.record(20, 0, 0);

    assertEquals(120, metric.value());
    assertEquals(0, metric.cpuNanos());
    assertEquals(0, metric.memoryBytes());
  }

  @Test
  void profileScopeRecordsResourcesAndSummaryRollsUp() {
    QueryProfiling.activate(true);

    // Burn a little CPU and allocate inside a phase scope so the deltas are observable when the
    // JVM supports per-thread metrics.
    try (ProfileScope ignored = ProfileScope.open(MetricName.EXECUTE)) {
      long acc = 0;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < 200_000; i++) {
        acc += i;
        if ((i & 0x3FFF) == 0) {
          sb.append(i);
        }
      }
      // Prevent the JIT from eliminating the work above.
      if (acc == Long.MIN_VALUE) {
        throw new IllegalStateException(sb.toString());
      }
    }

    QueryProfile profile = QueryProfiling.current().finish();

    QueryProfile.Phase execute = profile.getPhases().get("execute");
    assertTrue(execute.getTimeMillis() >= 0d, "time is recorded");

    // The summary CPU/memory totals must equal the sum across phases (only EXECUTE ran here).
    double phaseCpuSum =
        profile.getPhases().values().stream()
            .mapToDouble(QueryProfile.Phase::getCpuTimeMillis)
            .sum();
    long phaseMemSum =
        profile.getPhases().values().stream().mapToLong(QueryProfile.Phase::getMemoryBytes).sum();
    assertEquals(phaseCpuSum, profile.getSummary().getTotalCpuTimeMillis(), 0.0001);
    assertEquals(phaseMemSum, profile.getSummary().getTotalMemoryBytes());

    // CPU/memory are non-negative regardless of whether the JVM exposes the metrics.
    assertTrue(execute.getCpuTimeMillis() >= 0d);
    assertTrue(execute.getMemoryBytes() >= 0L);
  }
}
