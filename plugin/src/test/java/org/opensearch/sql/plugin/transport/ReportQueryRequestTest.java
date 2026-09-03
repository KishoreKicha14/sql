/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

/**
 * Round-trip serialization tests for the producer-side {@link ReportQueryRequest} / {@link
 * ReportQueryResponse}.
 *
 * <p>The wire format is duplicated (not shared) with the Query Insights plugin's copy. The Query
 * Insights plugin has an equivalent test asserting the same field values and order; both must stay
 * in sync so the two mirrored classes remain wire-compatible.
 */
public class ReportQueryRequestTest {

  @Test
  public void testRequestRoundTripWithPhases() throws IOException {
    List<ReportQueryRequest.PhaseMetric> phases =
        List.of(
            new ReportQueryRequest.PhaseMetric("prepare", 1.5, 0.5, 1024L),
            new ReportQueryRequest.PhaseMetric("analyze", 2.0, 1.0, 2048L),
            new ReportQueryRequest.PhaseMetric("execute", 10.0, 8.0, 4096L));
    ReportQueryRequest request =
        new ReportQueryRequest(
            "PPL",
            "node-1:42",
            "source=employees | where age > 30 | fields name",
            "source=employees | where age > ? | fields name",
            13L,
            9500L,
            7168L,
            1_700_000_000_000L,
            phases);

    ReportQueryRequest out = roundTrip(request);

    assertEquals("PPL", out.getQuerySource());
    assertEquals("node-1:42", out.getCoordinatorId());
    assertEquals("source=employees | where age > 30 | fields name", out.getQueryText());
    assertEquals("source=employees | where age > ? | fields name", out.getQueryShape());
    assertEquals(13L, out.getLatencyMillis());
    assertEquals(9500L, out.getCpuNanos());
    assertEquals(7168L, out.getMemoryBytes());
    assertEquals(1_700_000_000_000L, out.getTimestampMillis());
    assertEquals(3, out.getPhases().size());
    assertEquals("prepare", out.getPhases().get(0).getName());
    assertEquals(1.5, out.getPhases().get(0).getTimeMillis(), 0.0);
    assertEquals(0.5, out.getPhases().get(0).getCpuTimeMillis(), 0.0);
    assertEquals(1024L, out.getPhases().get(0).getMemoryBytes());
    assertEquals("execute", out.getPhases().get(2).getName());
    assertEquals(4096L, out.getPhases().get(2).getMemoryBytes());
  }

  @Test
  public void testRequestRoundTripWithoutPhasesAndNullShape() throws IOException {
    ReportQueryRequest request =
        new ReportQueryRequest("PPL", "node-1:7", "source=t", null, 0L, 0L, 0L, 123L, null);

    ReportQueryRequest out = roundTrip(request);

    assertNull(out.getQueryShape());
    assertTrue(out.getPhases().isEmpty());
    assertEquals("node-1:7", out.getCoordinatorId());
    assertNull(request.validate());
  }

  @Test
  public void testResponseRoundTrip() throws IOException {
    for (boolean ack : new boolean[] {true, false}) {
      ReportQueryResponse response = new ReportQueryResponse(ack);
      try (BytesStreamOutput out = new BytesStreamOutput()) {
        response.writeTo(out);
        try (StreamInput in = out.bytes().streamInput()) {
          assertEquals(ack, new ReportQueryResponse(in).isAcknowledged());
        }
      }
    }
  }

  private static ReportQueryRequest roundTrip(ReportQueryRequest request) throws IOException {
    try (BytesStreamOutput out = new BytesStreamOutput()) {
      request.writeTo(out);
      try (StreamInput in = out.bytes().streamInput()) {
        return new ReportQueryRequest(in);
      }
    }
  }
}
