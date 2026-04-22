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

import static org.junit.jupiter.api.Assertions.*;

import com.google.adk.events.Event;
import io.reactivex.rxjava3.core.Flowable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoopAgentRunLiveImplTest {

  private static final String ERROR_MESSAGE = "runLive is not defined for LoopAgent yet.";

  private LoopAgent buildAgent(
      Optional<Integer> maxIterations, List<? extends BaseAgent> subAgents) {
    // Assuming a LoopAgent.Builder exists with setName, setDescription, setSubAgents,
    // setMaxIterations,
    // build() or similar methods. Replace with actual builder/setup as required by
    // real LoopAgent.
    // TODO: Replace with actual builder usage if available.
    return new LoopAgent(
        "AgentName", // name (TODO: adjust if required)
        "description", // description (TODO: adjust if required)
        subAgents, // subAgents
        maxIterations, // maxIterations
        Collections.emptyList(), // beforeAgentCallback
        Collections.emptyList() // afterAgentCallback
        );
  }

  @Test
  @Tag("invalid")
  public void testRunLiveImplAlwaysThrowsUnsupportedOperationException() {
    LoopAgent agent = buildAgent(Optional.of(3), Collections.emptyList());
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    UnsupportedOperationException thrown =
        assertThrows(
            UnsupportedOperationException.class,
            () -> flowable.blockingFirst() // Will throw immediately as Flowable emits
            // an error
            );
    assertEquals(ERROR_MESSAGE, thrown.getMessage());
  }

  @Test
  @Tag("boundary")
  public void testRunLiveImplIgnoresMaxIterations() {
    Optional<Integer>[] maxIterationOpts =
        new Optional[] {
          Optional.empty(), Optional.of(0), Optional.of(1), Optional.of(Integer.MAX_VALUE)
        };
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    for (Optional<Integer> maxOpt : maxIterationOpts) {
      LoopAgent agent = buildAgent(maxOpt, Collections.emptyList());
      Flowable<Event> flowable = agent.runLiveImpl(ctx);
      UnsupportedOperationException thrown =
          assertThrows(UnsupportedOperationException.class, flowable::blockingFirst);
      assertEquals(ERROR_MESSAGE, thrown.getMessage());
    }
  }

  @Test
  @Tag("integration")
  public void testRunLiveImplDoesNotRunSubAgents() {
    // Create a dummy/mock sub-agent instance
    BaseAgent mockSubAgent = Mockito.mock(BaseAgent.class);
    List<BaseAgent> subAgents = Collections.singletonList(mockSubAgent);
    LoopAgent agent = buildAgent(Optional.of(3), subAgents);
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    UnsupportedOperationException thrown =
        assertThrows(UnsupportedOperationException.class, flowable::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrown.getMessage());
    // Ensure the subAgent is not run (no method calls made on it, since runLiveImpl
    // is unimplemented)
    Mockito.verifyNoInteractions(mockSubAgent);
  }

  @Test
  @Tag("boundary")
  public void testRunLiveImplWithNullOrEdgeInvocationContext() {
    LoopAgent agent = buildAgent(Optional.of(3), Collections.emptyList());
    // Case 1: Null InvocationContext
    Flowable<Event> flowableNullCtx = agent.runLiveImpl(null);
    UnsupportedOperationException thrownNull =
        assertThrows(UnsupportedOperationException.class, flowableNullCtx::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrownNull.getMessage());
    // Case 2: Minimal mock InvocationContext
    InvocationContext minimalCtx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowableMockCtx = agent.runLiveImpl(minimalCtx);
    UnsupportedOperationException thrownMock =
        assertThrows(UnsupportedOperationException.class, flowableMockCtx::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrownMock.getMessage());
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplReturnsColdFlowable() {
    LoopAgent agent = buildAgent(Optional.of(3), Collections.emptyList());
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    // The error should NOT be thrown until subscription occurs
    // No need to catch here, just assert that no exception thrown on creation.
    assertNotNull(flowable);
    // Subscribe and assert it errors only now
    UnsupportedOperationException thrown =
        assertThrows(UnsupportedOperationException.class, flowable::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrown.getMessage());
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplConsistentExceptionMessage() {
    LoopAgent agent = buildAgent(Optional.empty(), Collections.emptyList());
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    UnsupportedOperationException thrown =
        assertThrows(UnsupportedOperationException.class, flowable::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrown.getMessage());
  }

  @Test
  @Tag("valid")
  public void testRunLiveImplEmitsNoEvents() {
    LoopAgent agent = buildAgent(Optional.of(3), Collections.emptyList());
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    // Collect events before error
    List<Event> events =
        flowable
            .doOnError(
                e -> {
                  /* no-op */
                })
            .onErrorReturnItem(null) // Will emit null if error occurs
            .filter(e -> e != null)
            .toList()
            .blockingGet();
    // Asserts
    assertTrue(events.isEmpty());
    // Standard error check
    Flowable<Event> flowForError = agent.runLiveImpl(ctx);
    UnsupportedOperationException thrown =
        assertThrows(UnsupportedOperationException.class, flowForError::blockingFirst);
    assertEquals(ERROR_MESSAGE, thrown.getMessage());
  }

  @Test
  @Tag("integration")
  public void testRunLiveImplErrorPropagatesThroughOperators() {
    LoopAgent agent = buildAgent(Optional.of(1), Collections.emptyList());
    InvocationContext ctx = Mockito.mock(InvocationContext.class);
    Flowable<Event> flowable = agent.runLiveImpl(ctx);
    // Map operator
    Flowable<Event> mapped =
        flowable.map(
            e -> {
              fail("Map should not be called");
              return e;
            });
    assertThrows(UnsupportedOperationException.class, mapped::blockingFirst);
    // FlatMap operator
    Flowable<Event> flatMapped = flowable.flatMap(e -> Flowable.just(e));
    assertThrows(UnsupportedOperationException.class, flatMapped::blockingFirst);
    // Take operator
    Flowable<Event> takeOne = flowable.take(1);
    assertThrows(UnsupportedOperationException.class, takeOne::blockingFirst);
    // First operator
    Flowable<Event> firstOp = flowable.firstElement().toFlowable();
    assertThrows(UnsupportedOperationException.class, firstOp::blockingFirst);
  }
}
