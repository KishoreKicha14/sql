/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

/**
 * Named metrics used by query profiling. Ordering here is also the display order of the phase
 * breakdown: prepare (parse + context/plan setup) → analyze (semantic analysis) → optimize (plan
 * conversion + Calcite optimization) → execute → format.
 */
public enum MetricName {
  PREPARE,
  ANALYZE,
  OPTIMIZE,
  EXECUTE,
  FORMAT
}
