/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.sql.monitor.profile.ProfileCapturingTask;
import org.opensearch.sql.monitor.profile.QueryProfile;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask implements ProfileCapturingTask {

  /**
   * Per-phase profile snapshot for this query, stashed on the execution thread just before the
   * profiling thread-local is cleared, so the resource-tracking completion listener can read it
   * when it reports the query to Query Insights. Volatile because it is written on the execution
   * thread and read on the completion-listener thread.
   */
  private volatile QueryProfile queryProfile;

  public PPLQueryTask(
      long id,
      String type,
      String action,
      String description,
      TaskId parentTaskId,
      Map<String, String> headers) {
    super(id, type, action, description, parentTaskId, headers);
  }

  @Override
  public boolean shouldCancelChildrenOnCancellation() {
    return true;
  }

  /**
   * Enable per-thread CPU/memory accounting for the PPL coordinator task so its resource usage (and
   * the PPL engine work attributed to it on the {@code sql-worker} pool) is exposed through the
   * tasks API and consumed by Query Insights. Defaults to {@code false} on {@link CancellableTask},
   * which is why it must be overridden here.
   */
  @Override
  public boolean supportsResourceTracking() {
    return true;
  }

  /** The per-phase profile snapshot for this query, or {@code null} if not yet captured. */
  public QueryProfile getQueryProfile() {
    return queryProfile;
  }

  @Override
  public void setQueryProfile(QueryProfile queryProfile) {
    this.queryProfile = queryProfile;
  }
}
