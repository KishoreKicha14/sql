/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.transport.BytesTransportRequest;
import org.opensearch.transport.EmptyTransportResponseHandler;
import org.opensearch.transport.TransportService;

/**
 * Serializes a completed PPL/SQL query record and hands it to Query Insights over the transport
 * layer so it flows through Query Insights' in-memory {@code addRecord} pipeline — appearing in the
 * in-memory Top N, the historical index, and the ingest-time roll-up.
 *
 * <p><b>Why a transport send of a core request type instead of a direct index write or a
 * {@code client.execute}.</b> The record is carried as the <b>core</b>
 * {@link BytesTransportRequest}. That single class identity is shared by the SQL plugin classloader
 * and the Query Insights plugin classloader, so a same-node delivery is not subject to the
 * {@link ClassCastException} a plugin-defined request type would hit. {@code BytesTransportRequest}
 * extends {@code TransportRequest} (not {@code ActionRequest}), so it is sent with
 * {@link TransportService#sendRequest} against the action name Query Insights registers a raw
 * request handler for — not via {@code client.execute}/an {@code ActionType}.
 *
 * <p><b>Action name and wire format.</b> Query Insights owns both. This class mirrors them; the two
 * MUST stay in lock-step (see the Query Insights {@code ReportQueryBytesAction} for the canonical
 * definition). The action name and format version are duplicated here as string/int literals rather
 * than referenced, because the SQL plugin does not (and should not) depend on the Query Insights
 * plugin artifact.
 */
public final class QueryInsightsReporter {

  private static final Logger LOG = LogManager.getLogger(QueryInsightsReporter.class);

  /**
   * Transport action name. MUST match {@code ReportQueryBytesAction.NAME} in Query Insights.
   */
  public static final String ACTION_NAME = "cluster:admin/opensearch/query_insights/report_query_bytes";

  /**
   * Wire format version. MUST match {@code ReportQueryBytesAction.FORMAT_VERSION} in Query Insights.
   * The two plugins are unreleased and always built together, so there is a single format: any
   * change to the layout below is a coordinated change on both sides, not a compatibility boundary.
   */
  public static final int FORMAT_VERSION = 1;

  private QueryInsightsReporter() {}

  /**
   * Serialize the record and send it to the local node's Query Insights handler. Best-effort: any
   * failure (including Query Insights not being installed, so the action is unregistered) is logged
   * at debug and swallowed so it never affects query execution.
   *
   * @param transportService the transport service used to send the request
   * @param localNode the local (coordinator) node — the handler is registered on every node, so we
   *     send to ourselves
   * @param querySource the query source label (e.g. {@code "PPL"})
   * @param parentMarker the originating query's marker {@code <source>:<nodeId>:<taskId>}; used as
   *     both the record id and the parent marker so child DSL records (tagged with the same value
   *     via {@code DERIVED_FROM}) roll up into this record
   * @param nodeId the coordinator node id
   * @param queryText the (prefix-stripped) query text
   * @param timestampMillis the record timestamp
   * @param latencyMillis end-to-end coordinator latency
   * @param cpuNanos coordinator CPU nanos
   * @param memoryBytes coordinator memory bytes
   * @param phases per-phase breakdown: name -&gt; {time_ms(double), cpu_time_ms(double),
   *     memory_bytes(long/Number)}
   * @param queryShapeHash normalized PPL/SQL shape hash for SIMILARITY grouping, prefixed to keep
   *     PPL and DSL groups in separate namespaces (e.g. {@code "ppl:<hash>"}); empty/null when not
   *     applicable
   */
  public static void report(
      TransportService transportService,
      DiscoveryNode localNode,
      String querySource,
      String parentMarker,
      String nodeId,
      String queryText,
      long timestampMillis,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      Map<String, Map<String, Object>> phases,
      String queryShapeHash) {
    try {
      final BytesStreamOutput out = new BytesStreamOutput();
      out.writeVInt(FORMAT_VERSION);
      out.writeString(querySource == null ? "" : querySource);
      out.writeString(parentMarker == null ? "" : parentMarker);
      out.writeString(nodeId == null ? "" : nodeId);
      out.writeString(queryText == null ? "" : queryText);
      out.writeVLong(timestampMillis);
      out.writeVLong(Math.max(0L, latencyMillis));
      out.writeVLong(Math.max(0L, cpuNanos));
      out.writeVLong(Math.max(0L, memoryBytes));

      final Map<String, Map<String, Object>> safePhases = phases == null ? Map.of() : phases;
      out.writeVInt(safePhases.size());
      for (Map.Entry<String, Map<String, Object>> e : safePhases.entrySet()) {
        out.writeString(e.getKey() == null ? "" : e.getKey());
        final Map<String, Object> p = e.getValue() == null ? Map.of() : e.getValue();
        out.writeDouble(asDouble(p.get("time_ms")));
        out.writeDouble(asDouble(p.get("cpu_time_ms")));
        out.writeVLong(Math.max(0L, asLong(p.get("memory_bytes"))));
      }

      // Normalized shape hash for SIMILARITY grouping (empty when not applicable).
      out.writeString(queryShapeHash == null ? "" : queryShapeHash);

      final BytesTransportRequest request = new BytesTransportRequest(out.bytes(), Version.CURRENT);
      transportService.sendRequest(
          localNode, ACTION_NAME, request, EmptyTransportResponseHandler.INSTANCE_SAME);
    } catch (Exception e) {
      // Query Insights may not be installed (action unregistered) or the send may fail; never
      // affect query execution.
      LOG.debug("Failed to report PPL query to Query Insights", e);
    }
  }

  private static double asDouble(Object o) {
    return (o instanceof Number) ? ((Number) o).doubleValue() : 0.0d;
  }

  private static long asLong(Object o) {
    return (o instanceof Number) ? ((Number) o).longValue() : 0L;
  }
}
