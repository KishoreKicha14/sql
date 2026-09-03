/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

/**
 * Constants and helpers for the task header that links a child DSL search back to the SQL/PPL query
 * that spawned it, for Query Insights.
 *
 * <p>The header carries the originating query's source and coordinator task id as a single value of
 * the form {@code <source>:<nodeId>:<taskId>} (e.g. {@code PPL:node-1:42}). It is registered via
 * {@code SQLPlugin.getTaskHeaders()} so OpenSearch copies it from the coordinator thread context
 * into the child search tasks it spawns, including on remote data nodes, where Query Insights reads
 * it off the child {@code SearchTask} to (a) classify the child's query source and (b) associate it
 * with the parent query.
 *
 * <p>The name is source-neutral so the same mechanism serves PPL today and SQL later.
 */
public final class QueryInsightsMarker {

  /** Task header name carrying {@code <source>:<nodeId>:<taskId>} of the originating query. */
  public static final String PARENT_HEADER = "X-Query-Insights-Parent";

  private QueryInsightsMarker() {}

  /**
   * Build the header value for a coordinator query.
   *
   * @param source query source label (e.g. {@code "PPL"})
   * @param nodeId coordinator node id
   * @param taskId coordinator task id
   * @return the {@code <source>:<nodeId>:<taskId>} header value
   */
  public static String value(String source, String nodeId, long taskId) {
    return source + ":" + nodeId + ":" + taskId;
  }
}
