/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.monitor.profile;

import java.util.Objects;
import java.util.function.Supplier;

/**
 * Thread-local holder for query profiling contexts.
 *
 * <p>Callers can enable or disable profiling per-thread via {@link #activate(boolean)} and obtain
 * the active context with {@link #current()}.
 */
public final class QueryProfiling {

  private static final ThreadLocal<ProfileContext> CURRENT = new ThreadLocal<>();

  private QueryProfiling() {}

  /**
   * @return the profiling context bound to this thread, or a no-op context if not activated.
   */
  public static ProfileContext current() {
    ProfileContext ctx = CURRENT.get();
    return ctx == null ? NoopProfileContext.INSTANCE : ctx;
  }

  /**
   * Create noop profiling for the current thread.
   *
   * @return newly activated profiling context
   */
  public static ProfileContext noop() {
    return activate(false);
  }

  /**
   * Activate profiling for the current thread.
   *
   * @param profilingEnabled whether profiling should be enabled
   * @return newly activated profiling context
   */
  public static ProfileContext activate(boolean profilingEnabled) {
    return activate(profilingEnabled, profilingEnabled);
  }

  /**
   * Activate profiling for the current thread with independent control over per-phase capture and
   * the expensive per-operator plan-node profiling.
   *
   * <p>Per-phase resource capture (time/CPU/memory) is cheap and can be left on for every query so
   * Query Insights always gets the per-phase breakdown. Plan-node profiling rewrites the plan with
   * per-operator instrumentation that records per-node time/rows on every row iteration — the
   * expensive part — and should be enabled only when the user requested {@code profile=true}.
   *
   * @param phaseCaptureEnabled whether to collect cheap per-phase time/CPU/memory metrics
   * @param planProfilingEnabled whether to also enable the expensive per-operator plan profiling
   * @return newly activated profiling context
   */
  public static ProfileContext activate(boolean phaseCaptureEnabled, boolean planProfilingEnabled) {
    if (phaseCaptureEnabled) {
      CURRENT.set(new DefaultProfileContext(planProfilingEnabled));
    } else {
      CURRENT.set(NoopProfileContext.INSTANCE);
    }
    return CURRENT.get();
  }

  /** Clear any profiling context bound to the current thread. */
  public static void clear() {
    CURRENT.remove();
  }

  /**
   * Set the profiling context for the current thread. Used when propagating context across thread
   * boundaries.
   *
   * @param ctx profiling context to bind
   */
  public static void set(ProfileContext ctx) {
    CURRENT.set(Objects.requireNonNull(ctx, "ctx"));
  }

  /**
   * Run a supplier with the provided profiling context bound to the current thread.
   *
   * @param action supplier to execute
   * @return supplier result
   */
  public static <T> T withCurrentContext(ProfileContext ctx, Supplier<T> action) {
    CURRENT.set(Objects.requireNonNull(ctx, "ctx"));
    try {
      return action.get();
    } finally {
      clear();
    }
  }
}
