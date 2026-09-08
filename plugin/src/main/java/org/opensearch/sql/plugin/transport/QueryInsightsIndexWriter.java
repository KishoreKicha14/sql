/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.transport.client.Client;

/**
 * Best-effort writer that indexes a completed PPL query as a Query Insights "top queries" record so
 * it surfaces in the Query Insights historical Top N view.
 *
 * <p><b>Why a direct index write.</b> The live Top N overview is served from an in-memory structure
 * inside the Query Insights plugin that is only reachable via {@code
 * QueryInsightsService.addRecord}, which is on the Query Insights plugin's classloader and cannot
 * be called from this plugin. A same-node transport action cannot bridge that gap either: it passes
 * the request by reference, so the two plugins' request classes are different classes and the cast
 * fails. Writing the record directly into the Query Insights {@code top_queries-*} index sidesteps
 * the classloader boundary entirely (it is a plain OpenSearch index write using core types) and
 * makes the record visible in the Query Insights <em>historical</em> Top N read (the API path that
 * queries the index when a {@code from}/{@code to} range is supplied).
 *
 * <p>This class deliberately depends on nothing from the Query Insights plugin. The index name
 * scheme and document schema below mirror the Query Insights exporter/reader contract; they must be
 * kept in sync with Query Insights if that contract changes.
 */
final class QueryInsightsIndexWriter {

  private static final Logger LOG = LogManager.getLogger(QueryInsightsIndexWriter.class);

  /** Index name prefix used by Query Insights for the local top-queries index. */
  private static final String INDEX_PREFIX = "top_queries";

  /** Date pattern for the index name (UTC), matching the Query Insights exporter. */
  private static final DateTimeFormatter INDEX_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy.MM.dd", Locale.ROOT);

  /** Date pattern used to derive the 5-digit index-name hash, matching Query Insights. */
  private static final DateTimeFormatter HASH_DATE_FORMAT =
      DateTimeFormatter.ofPattern("yyyy-MM-dd", Locale.ROOT);

  private QueryInsightsIndexWriter() {}

  /**
   * Compute the current-day Query Insights top-queries index name, e.g. {@code
   * top_queries-2026.09.03-04060}. Mirrors {@code IndexDiscoveryHelper.buildLocalIndexName} and
   * {@code ExporterReaderUtils.generateLocalIndexDateHash} in Query Insights.
   */
  static String currentIndexName(ZonedDateTime nowUtc) {
    String datePart = INDEX_DATE_FORMAT.format(nowUtc);
    String hashInput = HASH_DATE_FORMAT.format(nowUtc.toLocalDate());
    int hash = (hashInput.hashCode() % 100000 + 100000) % 100000;
    return INDEX_PREFIX + "-" + datePart + "-" + String.format(Locale.ROOT, "%05d", hash);
  }

  /**
   * Build the top-queries document body for a completed PPL query. Only the fields Query Insights
   * needs to rank and render a latency Top N entry are populated; measurements carry a {@code NONE}
   * aggregation type (single, non-aggregated sample) matching the Query Insights model.
   */
  static XContentBuilder buildDocument(
      String id,
      String nodeId,
      String coordinatorId,
      String queryText,
      long timestampMillis,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      List<String> indices,
      Map<String, Map<String, Object>> phases)
      throws Exception {
    XContentBuilder builder = XContentFactory.jsonBuilder().startObject();
    builder.field("id", id);
    builder.field("timestamp", timestampMillis);
    builder.field("node_id", nodeId);
    builder.field("query_source", "PPL");
    // Self-marker so co-located child DSL records (tagged by Query Insights via the parent header)
    // can be associated with this parent in the detail view: "<source>:<nodeId>:<taskId>".
    builder.field("parent_marker", "PPL:" + coordinatorId);
    builder.field("is_child", false);
    builder.field("group_by", "NONE");
    builder.field("search_type", "query_then_fetch");
    builder.field("source", queryText);
    builder.field("source_truncated", false);
    builder.field("failed", false);

    if (indices != null && !indices.isEmpty()) {
      builder.field("indices", indices);
    }

    // Which Top N rankings this record participates in. Latency is always meaningful for PPL.
    builder.startObject("top_n_query");
    builder.field("latency", true);
    builder.field("cpu", true);
    builder.field("memory", true);
    builder.endObject();

    builder.startObject("measurements");
    writeMeasurement(builder, "latency", latencyMillis);
    writeMeasurement(builder, "cpu", cpuNanos);
    writeMeasurement(builder, "memory", memoryBytes);
    builder.endObject();

    if (phases != null && !phases.isEmpty()) {
      builder.field("phases", phases);
    }

    builder.endObject();
    return builder;
  }

  private static void writeMeasurement(XContentBuilder builder, String name, double number)
      throws Exception {
    builder.startObject(name);
    builder.field("number", number);
    builder.field("count", 1);
    builder.field("aggregationType", "NONE");
    builder.endObject();
  }

  /**
   * Best-effort index the record. Any failure (Query Insights not installed, index creation race,
   * write rejection) is swallowed so it can never affect PPL query execution.
   */
  static void write(
      Client client,
      String nodeId,
      String coordinatorId,
      String queryText,
      long timestampMillis,
      long latencyMillis,
      long cpuNanos,
      long memoryBytes,
      List<String> indices,
      Map<String, Map<String, Object>> phases) {
    try {
      String indexName = currentIndexName(ZonedDateTime.now(ZoneOffset.UTC));
      String id = UUID.randomUUID().toString();
      XContentBuilder doc =
          buildDocument(
              id,
              nodeId,
              coordinatorId,
              queryText,
              timestampMillis,
              latencyMillis,
              cpuNanos,
              memoryBytes,
              indices,
              phases);
      IndexRequest indexRequest = new IndexRequest(indexName).id(id).source(doc);
      client.index(
          indexRequest,
          ActionListener.wrap(
              r -> {},
              e -> {
                // The index may not exist yet on the first write; create it with the Query Insights
                // mapping and retry once.
                createIndexAndRetry(client, indexName, indexRequest, e);
              }));
    } catch (Exception e) {
      LOG.debug("Failed to write PPL query to Query Insights top-queries index", e);
    }
  }

  private static void createIndexAndRetry(
      Client client, String indexName, IndexRequest indexRequest, Exception firstError) {
    try {
      CreateIndexRequest createIndexRequest =
          new CreateIndexRequest(indexName).mapping(topQueriesMapping());
      client
          .admin()
          .indices()
          .create(
              createIndexRequest,
              ActionListener.wrap(
                  created -> client.index(indexRequest, ActionListener.wrap(r -> {}, e -> {})),
                  createErr -> {
                    // Another writer may have created it concurrently; try the write once more.
                    client.index(indexRequest, ActionListener.wrap(r -> {}, e -> {}));
                  }));
    } catch (Exception e) {
      LOG.debug("Failed to create Query Insights top-queries index [{}]", indexName, e);
    }
  }

  /**
   * Minimal mapping compatible with the Query Insights {@code top-queries-record.json}. Uses {@code
   * dynamic:true} plus the {@code _meta} tag Query Insights uses to identify its own indices, so
   * the created index is recognized by Query Insights lifecycle/discovery. Explicitly typed fields
   * match the Query Insights schema for the ones we populate; the rest are dynamic.
   */
  private static String topQueriesMapping() {
    return "{\"dynamic\":true,"
               + "\"_meta\":{\"schema_version\":1,\"query_insights_feature_space\":\"top_n_queries\"},"
               + "\"properties\":{\"id\":{\"type\":\"keyword\"},"
               + "\"timestamp\":{\"type\":\"date\",\"format\":\"epoch_millis\"},"
               + "\"node_id\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},"
               + "\"query_source\":{\"type\":\"keyword\"},\"derived_from\":{\"type\":\"keyword\"},"
               + "\"parent_marker\":{\"type\":\"keyword\"},\"is_child\":{\"type\":\"boolean\"},"
               + "\"group_by\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},"
               + "\"indices\":{\"type\":\"keyword\"},"
               + "\"search_type\":{\"type\":\"text\",\"fields\":{\"keyword\":{\"type\":\"keyword\",\"ignore_above\":256}}},"
               + "\"source\":{\"type\":\"match_only_text\",\"index\":false},"
               + "\"source_truncated\":{\"type\":\"boolean\"},\"failed\":{\"type\":\"boolean\"},"
               + "\"phases\":{\"type\":\"object\",\"enabled\":true},"
               + "\"measurements\":{\"properties\":{"
               + "\"latency\":{\"properties\":{\"number\":{\"type\":\"double\"},\"count\":{\"type\":\"integer\"},\"aggregationType\":{\"type\":\"keyword\"}}},"
               + "\"cpu\":{\"properties\":{\"number\":{\"type\":\"double\"},\"count\":{\"type\":\"integer\"},\"aggregationType\":{\"type\":\"keyword\"}}},"
               + "\"memory\":{\"properties\":{\"number\":{\"type\":\"double\"},\"count\":{\"type\":\"integer\"},\"aggregationType\":{\"type\":\"keyword\"}}}"
               + "}},\"top_n_query\":{\"properties\":{\"cpu\":{\"type\":\"boolean\"},\"latency\":{\"type\":\"boolean\"},\"memory\":{\"type\":\"boolean\"}}}"
               + "}}";
  }
}
