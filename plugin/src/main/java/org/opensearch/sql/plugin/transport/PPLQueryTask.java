/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.util.Map;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.tasks.CancellableTask;

public class PPLQueryTask extends CancellableTask {

  /**
   * Task header stamped on every child DSL search a PPL query issues, carrying the PPL coordinator
   * task id as {@code nodeId:taskId}. Registered via {@code SQLPlugin.getTaskHeaders()} so
   * OpenSearch copies it into child search tasks (including on remote data nodes), letting Query
   * Insights associate those searches back to the originating PPL query.
   */
  public static final String PPL_COORDINATOR_ID_HEADER = "X-PPL-Coordinator-Id";

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
}
