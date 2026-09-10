/*
 * Copyright 2021 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.couchbase.connect.kafka.util;

import com.couchbase.client.core.error.CouchbaseException;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class KafkaRetryHelperTest {
  private TestClock clock;
  private KafkaRetryHelper retryHelper;

  @BeforeEach
  public void setup() {
    clock = new TestClock();
    retryHelper = new KafkaRetryHelper("test", Duration.ofSeconds(5), clock);
  }

  @AfterEach
  public void after() {
    retryHelper.close();
  }

  private static class TestClock implements KafkaRetryHelper.Clock {
    public long now = Long.MAX_VALUE;

    @Override
    public long nanoTime() {
      return now;
    }

    public void advance(Duration d) {
      now += d.toNanos();
    }
  }

  private static void throwCouchbaseException() {
    throw new CouchbaseException("oops!");
  }

  // A recognizable, synthetic stand-in for the record key that the Couchbase SDK's
  // KeyValueErrorContext renders as "documentId" inside its JSON exception message.
  private static final String CANARY_DOC_ID = "canary-document-id-2f9a7c1b";

  private static void throwCouchbaseExceptionWithDocId() {
    throw new CouchbaseException(
        "UpsertRequest, Reason: TIMEOUT {\"service\":{\"type\":\"kv\",\"documentId\":\"" + CANARY_DOC_ID + "\"}}");
  }

  private static void assertNoDocumentIdLeak(Throwable thrown) {
    for (Throwable t = thrown; t != null; t = t.getCause()) {
      String message = t.getMessage();
      assertFalse(message != null && message.contains(CANARY_DOC_ID),
          "record documentId leaked via " + t.getClass().getName() + ": " + message);
    }
  }

  private void assertRetriable() {
    assertThrows(RetriableException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
  }

  private void assertNotRetriable() {
    ConnectException thrown = assertThrows(ConnectException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
    assertFalse(thrown instanceof RetriableException,
        "terminal failure must be non-retriable so the task actually fails");
  }

  private void assertSuccess() {
    AtomicBoolean b = new AtomicBoolean();
    retryHelper.runWithRetry(() -> b.set(true));
    assertTrue(b::get);
  }

  @Test
  public void eventuallyTimesOut() {
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    clock.advance(Duration.ofSeconds(4));
    assertNotRetriable();
  }

  @Test
  public void successResetsRetryStartTime() {
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    assertSuccess();

    clock.advance(Duration.ofSeconds(4));
    assertRetriable();

    clock.advance(Duration.ofSeconds(1));
    assertRetriable();

    clock.advance(Duration.ofSeconds(4));
    assertNotRetriable();
  }

  @Test
  public void canSucceedImmediately() {
    assertSuccess();
    clock.advance(Duration.ofDays(1));
    assertSuccess();
    clock.advance(Duration.ofDays(1));
    assertSuccess();
  }

  @Test
  public void zeroRetryDurationMeansNoRetry() {
    try (KafkaRetryHelper retryHelper = new KafkaRetryHelper("test", Duration.ZERO, clock)) {
      ConnectException thrown = assertThrows(ConnectException.class, () ->
          retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseException));
      assertFalse(thrown instanceof RetriableException);
    }
  }

  @Test
  public void retriablePathDoesNotLeakDocumentId() {
    RetriableException thrown = assertThrows(RetriableException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseExceptionWithDocId));
    assertNull(thrown.getCause(), "the raw SDK exception must not ride along as the cause");
    assertNoDocumentIdLeak(thrown);
  }

  @Test
  public void terminalPathDoesNotLeakDocumentId() {
    // Use up the retry budget so the terminal (non-retriable) branch runs.
    assertThrows(RetriableException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseExceptionWithDocId));
    clock.advance(Duration.ofSeconds(5));

    ConnectException thrown = assertThrows(ConnectException.class, () ->
        retryHelper.runWithRetry(KafkaRetryHelperTest::throwCouchbaseExceptionWithDocId));
    assertFalse(thrown instanceof RetriableException, "terminal failure must be non-retriable");
    assertNull(thrown.getCause());
    assertNoDocumentIdLeak(thrown);
  }

  @Test
  public void retryDisabledPathDoesNotLeakDocumentId() {
    try (KafkaRetryHelper noRetry = new KafkaRetryHelper("test", Duration.ZERO, clock)) {
      ConnectException thrown = assertThrows(ConnectException.class, () ->
          noRetry.runWithRetry(KafkaRetryHelperTest::throwCouchbaseExceptionWithDocId));
      assertFalse(thrown instanceof RetriableException);
      assertNull(thrown.getCause());
      assertNoDocumentIdLeak(thrown);
    }
  }
}
