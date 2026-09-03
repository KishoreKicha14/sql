/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;

/**
 * Internal action used by the SQL/PPL plugin to report a completed non-DSL query (PPL today, SQL
 * later) to the Query Insights plugin for Top N / Live Queries processing.
 *
 * <p>This plugin only <em>sends</em> the action ({@code client.execute}); the handler is registered
 * by the Query Insights plugin. The two plugins do not share classes: the request/response wire
 * format is duplicated on both sides and kept in sync by convention (see {@link ReportQueryRequest}
 * for the versioned wire contract). If Query Insights is not installed the send fails and is
 * swallowed (best-effort).
 */
public class ReportQueryAction extends ActionType<ReportQueryResponse> {

  public static final String NAME = "cluster:admin/opensearch/query_insights/report_query";
  public static final ReportQueryAction INSTANCE = new ReportQueryAction();

  private ReportQueryAction() {
    super(NAME, ReportQueryResponse::new);
  }
}
