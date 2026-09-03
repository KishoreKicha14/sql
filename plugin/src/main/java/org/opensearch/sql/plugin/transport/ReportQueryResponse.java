/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import java.io.IOException;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

/**
 * Minimal acknowledgement for {@link ReportQueryAction}. Mirrored on the Query Insights side; keep
 * the single-boolean wire format in sync.
 */
public class ReportQueryResponse extends ActionResponse {

  private final boolean acknowledged;

  public ReportQueryResponse(boolean acknowledged) {
    this.acknowledged = acknowledged;
  }

  public ReportQueryResponse(StreamInput in) throws IOException {
    super(in);
    this.acknowledged = in.readBoolean();
  }

  @Override
  public void writeTo(StreamOutput out) throws IOException {
    out.writeBoolean(acknowledged);
  }

  public boolean isAcknowledged() {
    return acknowledged;
  }
}
