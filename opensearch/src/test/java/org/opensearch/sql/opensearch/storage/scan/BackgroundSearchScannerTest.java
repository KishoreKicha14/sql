/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage.scan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchNodeClient;
import org.opensearch.sql.opensearch.executor.OpenSearchQueryManager;
import org.opensearch.sql.opensearch.request.OpenSearchQueryRequest;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;
import org.opensearch.tasks.CancellableTask;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.client.node.NodeClient;

class BackgroundSearchScannerTest {
  private OpenSearchClient client;
  private NodeClient nodeClient;
  private ThreadPool threadPool;
  private OpenSearchRequest request;
  private BackgroundSearchScanner scanner;
  private ExecutorService executor;

  @BeforeEach
  void setUp() {
    client = mock(OpenSearchClient.class);
    nodeClient = mock(NodeClient.class);
    threadPool = mock(ThreadPool.class);
    request = mock(OpenSearchQueryRequest.class);
    executor = Executors.newSingleThreadExecutor();

    when(client.getNodeClient()).thenReturn(Optional.of(nodeClient));
    when(nodeClient.threadPool()).thenReturn(threadPool);
    when(threadPool.executor(any())).thenReturn(executor);

    scanner = new BackgroundSearchScanner(client, 10, 10);
  }

  @AfterEach
  void tearDown() throws Exception {
    OpenSearchQueryManager.clearCancellableTask();
    executor.shutdownNow();
  }

  @Test
  void testBackgroundSearchesRunUnderCallersTask() {
    CancellableTask task = mock(CancellableTask.class);
    OpenSearchQueryManager.setCancellableTask(task);
    List<CancellableTask> seen = new CopyOnWriteArrayList<>();
    OpenSearchResponse full = mockResponse(false, false, 10);
    OpenSearchResponse last = mockResponse(false, false, 5);
    when(client.search(request))
        .thenAnswer(
            invocation -> {
              seen.add(OpenSearchQueryManager.getCancellableTask());
              return seen.size() == 1 ? full : last;
            });

    scanner.startScanning(request); // initial fetch
    scanner.fetchNextBatch(request); // full page, so it prefetches the next batch
    scanner.fetchNextBatch(request);

    assertEquals(2, seen.size(), "both the initial fetch and the prefetch run in the background");
    assertSame(task, seen.get(0));
    assertSame(task, seen.get(1));
  }

  @Test
  void testBackgroundThreadDoesNotKeepTaskAfterSearch() throws Exception {
    OpenSearchQueryManager.setCancellableTask(mock(CancellableTask.class));
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);

    assertNull(
        executor.submit(OpenSearchQueryManager::getCancellableTask).get(),
        "a reused pool thread must not carry the previous query's task");
  }

  /**
   * End to end through the real node client: a search issued from the background pool carries the
   * query's task as its parent, which is what lets core cancel it with the query.
   */
  @Test
  @SuppressWarnings("unchecked")
  void testBackgroundSearchIsParentedToQueryTask() {
    NodeClient deepNodeClient = mock(NodeClient.class, RETURNS_DEEP_STUBS);
    when(deepNodeClient.threadPool().executor(any())).thenReturn(executor);
    when(deepNodeClient.getLocalNodeId()).thenReturn("node-1");
    SearchResponse searchResponse = mock(SearchResponse.class);
    when(searchResponse.getHits()).thenReturn(SearchHits.empty());
    when(deepNodeClient.search(any()).actionGet()).thenReturn(searchResponse);

    SearchRequest searchRequest = new SearchRequest();
    OpenSearchRequest nodeRequest = mock(OpenSearchRequest.class);
    when(nodeRequest.search(any(), any()))
        .thenAnswer(
            invocation -> {
              invocation
                  .<Function<SearchRequest, SearchResponse>>getArgument(0)
                  .apply(searchRequest);
              return OpenSearchResponse.EMPTY;
            });

    CancellableTask task = mock(CancellableTask.class);
    when(task.getId()).thenReturn(42L);
    OpenSearchQueryManager.setCancellableTask(task);

    BackgroundSearchScanner nodeScanner =
        new BackgroundSearchScanner(new OpenSearchNodeClient(deepNodeClient), 10, 10);
    nodeScanner.startScanning(nodeRequest);
    nodeScanner.fetchNextBatch(nodeRequest);

    assertEquals(new TaskId("node-1", 42L), searchRequest.getParentTask());
  }

  @Test
  void testSyncFallbackWhenNoNodeClient() {
    // Setup client without node client
    OpenSearchClient syncClient = mock(OpenSearchClient.class);
    when(syncClient.getNodeClient()).thenReturn(Optional.empty());
    scanner = new BackgroundSearchScanner(syncClient, 10, 10);

    OpenSearchResponse response = mockResponse(false, false, 10);
    when(syncClient.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertFalse(
        result.stopIteration(), "Expected iteration to continue after fetching one full page");
    verify(syncClient, times(1)).search(request);
  }

  @Test
  void testCompleteScanWithMultipleBatches() {
    // First batch: normal response
    OpenSearchResponse response1 = mockResponse(false, false, 10);
    // Second batch: empty response
    OpenSearchResponse response2 = mockResponse(true, false, 5);

    when(client.search(request)).thenReturn(response1).thenReturn(response2);

    scanner.startScanning(request);

    // First batch
    BackgroundSearchScanner.SearchBatchResult result1 = scanner.fetchNextBatch(request);
    assertFalse(
        result1.stopIteration(), "Expected iteration to continue after fetching 10/15 results");
    assertTrue(result1.iterator().hasNext());

    // Second batch
    BackgroundSearchScanner.SearchBatchResult result2 = scanner.fetchNextBatch(request);
    assertTrue(result2.stopIteration());
    assertFalse(result2.iterator().hasNext());
  }

  @Test
  void testFetchOnceForAggregationResponse() {
    OpenSearchResponse response = mockResponse(false, true, 1);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testFetchOnceWhenResultsBelowWindow() {
    OpenSearchResponse response = mockResponse(false, false, 5);
    when(client.search(request)).thenReturn(response);

    scanner.startScanning(request);
    BackgroundSearchScanner.SearchBatchResult result = scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());
  }

  @Test
  void testReset() {
    OpenSearchResponse response1 = mockResponse(false, false, 5);
    OpenSearchResponse response2 = mockResponse(true, false, 0);

    when(client.search(request)).thenReturn(response1).thenReturn(response2);

    scanner.startScanning(request);
    scanner.fetchNextBatch(request);
    scanner.fetchNextBatch(request);

    assertTrue(scanner.isScanDone());

    scanner.reset(request);

    assertFalse(scanner.isScanDone());
  }

  private OpenSearchResponse mockResponse(boolean isEmpty, boolean isAggregation, int numResults) {
    OpenSearchResponse response = mock(OpenSearchResponse.class);
    when(response.isEmpty()).thenReturn(isEmpty);
    when(response.isAggregationResponse()).thenReturn(isAggregation);

    if (numResults > 0) {
      ExprValue[] values = new ExprValue[numResults];
      Arrays.fill(values, mock(ExprValue.class));
      when(response.iterator()).thenReturn(Arrays.asList(values).iterator());
    } else {
      when(response.iterator()).thenReturn(Collections.emptyIterator());
    }

    when(response.getHitsSize()).thenReturn(numResults);
    return response;
  }
}
