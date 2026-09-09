/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/**
 * Implemented by a coordinator task that wants the query's per-phase {@link QueryProfile} snapshot
 * stashed on it while the profiling context is still bound to the execution thread.
 *
 * <p>Profiling accumulates into a thread-local {@link ProfileContext} on the engine's execution
 * ({@code sql-worker}) thread. The completion listener that reports the query to Query Insights runs
 * on a different thread, where {@link QueryProfiling#current()} returns the no-op context and the
 * per-phase breakdown is lost. Capturing the profile on the execution thread and stashing it here —
 * so the completion listener reads it off the task — keeps the phase breakdown intact.
 */
public interface ProfileCapturingTask {

  /**
   * Store the per-phase profile snapshot for this query.
   *
   * @param queryProfile the finished profile (may be an empty profile if profiling was inactive)
   */
  void setQueryProfile(QueryProfile queryProfile);
}
