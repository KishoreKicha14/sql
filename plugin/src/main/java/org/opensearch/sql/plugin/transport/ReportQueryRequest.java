/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Request carrying a completed query's metrics to the Query Insights plugin.
 *
 * <p><b>Wire contract (mirrored, keep in sync with the Query Insights consumer's copy).</b> The
 * SQL/PPL plugin and the Query Insights plugin do not share classes; each defines its own request
 * class implementing the <em>identical</em> byte layout. To evolve safely:
 *
 * <ul>
 *   <li>The first field is a {@code vInt} format version ({@link #FORMAT_VERSION}). Never reorder
 *       or remove existing fields — only append new fields after a version bump, and have readers
 *       branch on the version.
 *   <li>Use only stream primitives (no cross-plugin or engine-specific types).
 * </ul>
 *
 * <p>Fields (version 1): querySource, coordinatorId, queryText, queryShape (optional), latency
 * (ms), cpu (nanos), memory (bytes), timestamp (ms), and a length-prefixed list of per-phase
 * measurements {@code {name, timeMs, cpuMs, memBytes}}.
 */
public class ReportQueryRequest extends ActionRequest {

  /** Wire format version. Bump and append-only when adding fields. */
  public static final int FORMAT_VERSION = 1;

  private final String querySource;
  private final String coordinatorId;
  private final String queryText;
  private final String queryShape;
  private final long latencyMillis;
  private final long cpuNanos;
  private final long memoryBytes;
  private final long timestampMillis;
  private final List<PhaseMetric> phases;

  /** Immutable per-phase measurement carried on the wire. */
  public static class PhaseMetric {
    private final String name;
    private final double timeMillis;
    private final double cpuTimeMillis;
    private final long memoryBytes;

    public PhaseMetric(String name, double timeMillis, double cpuTimeMillis, long memoryBytes) {
      this.name = name;
      this.timeMillis = timeMillis;
      this.cpuTimeMillis = cpuTimeMillis;
      this.memoryBytes = memoryBytes;
    }

    public String getName() {
      return name;
    }

    public double getTimeMillis() {
      return timeMillis;
    }

    public double getCpuTimeMillis() {
      return cpuTimeMillis;
    }

    public long getMemoryBytes() {
      return memoryBytes;
    }
  }

  public ReportQueryRequest(
      String querySource,
      String coordinatorId,
      String queryText,
      String queryShape,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      long timestampMillis,
      List<PhaseMetric> phases) {
    this.querySource = querySource;
    this.coordinatorId = coordinatorId;
    this.queryText = queryText;
    this.queryShape = queryShape;
    this.latencyMillis = latencyMillis;
    this.cpuNanos = cpuNanos;
    this.memoryBytes = memoryBytes;
    this.timestampMillis = timestampMillis;
    this.phases = phases == null ? List.of() : phases;
  }

  public ReportQueryRequest(StreamInput in) throws IOException {
    super(in);
    int version = in.readVInt();
    // Version 1 is the only known format today. Future versions append fields; a v1 reader stops
    // after the v1 fields, which is safe because appended fields come after.
    this.querySource = in.readString();
    this.coordinatorId = in.readString();
    this.queryText = in.readString();
    this.queryShape = in.readOptionalString();
    this.latencyMillis = in.readVLong();
    this.cpuNanos = in.readVLong();
    this.memoryBytes = in.readVLong();
    this.timestampMillis = in.readVLong();
    int phaseCount = in.readVInt();
    List<PhaseMetric> read = new ArrayList<>(phaseCount);
    for (int i = 0; i < phaseCount; i++) {
      String name = in.readString();
      double timeMs = in.readDouble();
      double cpuMs = in.readDouble();
      long memBytes = in.readVLong();
      read.add(new PhaseMetric(name, timeMs, cpuMs, memBytes));
    }
    this.phases = read;
    assert version >= 1 : "unexpected ReportQueryRequest format version " + version;
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    super.writeTo(out);
    out.writeVInt(FORMAT_VERSION);
    out.writeString(querySource);
    out.writeString(coordinatorId);
    out.writeString(queryText);
    out.writeOptionalString(queryShape);
    out.writeVLong(latencyMillis);
    out.writeVLong(cpuNanos);
    out.writeVLong(memoryBytes);
    out.writeVLong(timestampMillis);
    out.writeVInt(phases.size());
    for (PhaseMetric phase : phases) {
      out.writeString(phase.getName());
      out.writeDouble(phase.getTimeMillis());
      out.writeDouble(phase.getCpuTimeMillis());
      out.writeVLong(phase.getMemoryBytes());
    }
  }

  @Override
  public ActionRequestValidationException validate() {
    return null;
  }

  public String getQuerySource() {
    return querySource;
  }

  public String getCoordinatorId() {
    return coordinatorId;
  }

  public String getQueryText() {
    return queryText;
  }

  public String getQueryShape() {
    return queryShape;
  }

  public long getLatencyMillis() {
    return latencyMillis;
  }

  public long getCpuNanos() {
    return cpuNanos;
  }

  public long getMemoryBytes() {
    return memoryBytes;
  }

  public long getTimestampMillis() {
    return timestampMillis;
  }

  public List<PhaseMetric> getPhases() {
    return phases;
  }
}
