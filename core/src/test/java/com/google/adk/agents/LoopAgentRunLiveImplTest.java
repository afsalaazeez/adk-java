/*
 * Copyright 2025 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.adk.agents;

import com.google.adk.events.Event;
import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoopAgentRunLiveImplTest {

  // Helper: create minimal BaseAgent mock for subAgents
  private BaseAgent createMockSubAgent() {
    return Mockito.mock(BaseAgent.class);
  }

  // Helper: minimal InvocationContext mock
  private InvocationContext createMockInvocationContext() {
    return Mockito.mock(InvocationContext.class);
  }

  // Helper: build a LoopAgent with provided maxIterations and subAgents
  private LoopAgent buildLoopAgent(
      Optional<Integer> maxIterations, List<? extends BaseAgent> subAgents) {
    // TODO: replace name/description with test-specific identifiers if required
    return new LoopAgent(
        "TestAgent",
        "Test Agent Description",
        subAgents,
        maxIterations,
        Collections.emptyList(),
        Collections.emptyList());
  }

  @Test
  @Tag("invalid")
  public void testRunLiveImplAlwaysThrowsUnsupportedOperationException() {
    LoopAgent agent = buildLoopAgent(Optional.empty(), ImmutableList.of());
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable = agent.runLiveImpl(context);
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    testSubscriber.assertError(UnsupportedOperationException.class);
    testSubscriber.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriber.assertNoValues();
  }

  @Test
  @Tag("boundary")
  public void testRunLiveImplIgnoresMaxIterations() {
    Optional<Integer>[] maxIterationsCases =
        new Optional[] {
          Optional.empty(), Optional.of(0), Optional.of(1), Optional.of(Integer.MAX_VALUE)
        };
    InvocationContext context = createMockInvocationContext();
    for (Optional<Integer> maxIterations : maxIterationsCases) {
      LoopAgent agent = buildLoopAgent(maxIterations, ImmutableList.of());
      Flowable<Event> flowable = agent.runLiveImpl(context);
      TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
      flowable.subscribe(testSubscriber);
      testSubscriber.assertError(UnsupportedOperationException.class);
      testSubscriber.assertError(
          e ->
              "runLive is not defined for LoopAgent yet."
                  .equals(((UnsupportedOperationException) e).getMessage()));
      testSubscriber.assertNoValues();
    }
  }

  @Test
  @Tag("boundary")
  public void testRunLiveImplDoesNotRunSubAgents() {
    List<BaseAgent> subAgents = ImmutableList.of(createMockSubAgent(), createMockSubAgent());
    LoopAgent agent = buildLoopAgent(Optional.empty(), subAgents);
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable = agent.runLiveImpl(context);
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    testSubscriber.assertError(UnsupportedOperationException.class);
    testSubscriber.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriber.assertNoValues();
  }

  @Test
  @Tag("boundary")
  public void testRunLiveImplWithNullOrEdgeInvocationContext() {
    LoopAgent agent = buildLoopAgent(Optional.empty(), ImmutableList.of());
    // Test with null InvocationContext
    Flowable<Event> flowableNull = agent.runLiveImpl(null);
    TestSubscriber<Event> testSubscriberNull = new TestSubscriber<>();
    flowableNull.subscribe(testSubscriberNull);
    testSubscriberNull.assertError(UnsupportedOperationException.class);
    testSubscriberNull.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriberNull.assertNoValues();
    // Test with minimal/mock InvocationContext
    InvocationContext minimalContext = createMockInvocationContext();
    Flowable<Event> flowableMinimal = agent.runLiveImpl(minimalContext);
    TestSubscriber<Event> testSubscriberMinimal = new TestSubscriber<>();
    flowableMinimal.subscribe(testSubscriberMinimal);
    testSubscriberMinimal.assertError(UnsupportedOperationException.class);
    testSubscriberMinimal.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriberMinimal.assertNoValues();
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplReturnsColdFlowable() {
    LoopAgent agent = buildLoopAgent(Optional.empty(), ImmutableList.of());
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable = agent.runLiveImpl(context);
    // No exception should have occurred at this point (not subscribed)
    // Now subscribe and assert the error is thrown
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    testSubscriber.assertError(UnsupportedOperationException.class);
    testSubscriber.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriber.assertNoValues();
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplConsistentExceptionMessage() {
    LoopAgent agent = buildLoopAgent(Optional.of(1), ImmutableList.of());
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable = agent.runLiveImpl(context);
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    testSubscriber.assertError(UnsupportedOperationException.class);
    testSubscriber.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriber.assertNoValues();
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplEmitsNoEvents() {
    LoopAgent agent = buildLoopAgent(Optional.of(2), ImmutableList.of());
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable = agent.runLiveImpl(context);
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    // Check that onNext was never called, only error
    testSubscriber.assertNoValues();
    testSubscriber.assertError(UnsupportedOperationException.class);
  }

  @Test
  @Tag("integration")
  public void testRunLiveImplErrorPropagatesThroughOperators() {
    LoopAgent agent = buildLoopAgent(Optional.empty(), ImmutableList.of());
    InvocationContext context = createMockInvocationContext();
    Flowable<Event> flowable =
        agent
            .runLiveImpl(context)
            .map(event -> event) // should not be called
            .take(1)
            .onErrorResumeNext(Flowable::error);
    TestSubscriber<Event> testSubscriber = new TestSubscriber<>();
    flowable.subscribe(testSubscriber);
    testSubscriber.assertError(UnsupportedOperationException.class);
    testSubscriber.assertError(
        e ->
            "runLive is not defined for LoopAgent yet."
                .equals(((UnsupportedOperationException) e).getMessage()));
    testSubscriber.assertNoValues();
  }
}
