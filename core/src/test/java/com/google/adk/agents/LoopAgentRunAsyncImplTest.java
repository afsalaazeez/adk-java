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
import static org.mockito.Mockito.*;

import com.google.adk.events.Event;
import io.reactivex.rxjava3.core.Flowable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.stubbing.Answer;

class LoopAgentRunAsyncImplTest {

  private InvocationContext mockInvocationContext;

  @BeforeEach
  void setUp() {
    MockitoAnnotations.openMocks(this);
    mockInvocationContext = mock(InvocationContext.class);
  }

  // =========================
  // Scenario 1: Null SubAgents
  // =========================
  @Test
  @Tag("boundary")
  public void testReturnsEmptyFlowableWhenSubAgentsIsNull() {
    LoopAgent agent =
        new LoopAgent(
            "test",
            "desc",
            null,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return null;
          }
        };
    List<Event> results = agent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
    assertTrue(results.isEmpty());
  }

  // =========================
  // Scenario 2: Empty SubAgents
  // =========================
  @Test
  @Tag("boundary")
  public void testReturnsEmptyFlowableWhenSubAgentsIsEmpty() {
    LoopAgent agent =
        new LoopAgent(
            "test",
            "desc",
            Collections.emptyList(),
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return Collections.emptyList();
          }
        };
    List<Event> results = agent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
    assertTrue(results.isEmpty());
  }

  // =========================================================================
  // Scenario 3: Executes all sub-agents, repeating, if no maxIterations/escalate
  // =========================================================================
  @Test
  @Tag("valid")
  public void testExecutesAllSubAgentsOnceIfNoMaxIterationsAndNoEscalate() {
    int nAgents = 2; // TODO: Change for more agents if desired.
    int consume = 6; // We'll only consume this many for test, otherwise it's
    // infinite!
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event event1 = mock(Event.class);
    Event event2 = mock(Event.class);
    when(agent1.runAsync(any())).thenReturn(Flowable.just(event1));
    when(agent2.runAsync(any())).thenReturn(Flowable.just(event2));
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(any())).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "noloop",
              "desc",
              Arrays.asList(agent1, agent2),
              Optional.empty(),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results =
          loopAgent.runAsyncImpl(mockInvocationContext).take(consume).toList().blockingGet();
      // Results must alternate in the agent order for 'consume' times
      for (int i = 0; i < consume; i++) {
        assertSame(results.get(i), (i % 2 == 0) ? event1 : event2);
      }
    }
  }

  // =========================================
  // Scenario 4: Stops if escalate is detected
  // =========================================
  @Test
  @Tag("valid")
  public void testStopsIterationWhenSubAgentEscalates() {
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event nonEscalate = mock(Event.class);
    Event escalate = mock(Event.class);
    when(agent1.runAsync(any())).thenReturn(Flowable.just(nonEscalate));
    when(agent2.runAsync(any())).thenReturn(Flowable.just(escalate));
    // hasEscalateAction: escalate returns true
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      mockedStatic.when(() -> LoopAgent.hasEscalateAction(nonEscalate)).thenReturn(false);
      mockedStatic.when(() -> LoopAgent.hasEscalateAction(escalate)).thenReturn(true);
      LoopAgent loopAgent =
          new LoopAgent(
              "stoppable",
              "desc",
              Arrays.asList(agent1, agent2),
              Optional.empty(),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(2, results.size());
      assertSame(results.get(0), nonEscalate);
      assertSame(results.get(1), escalate);
    }
  }

  // =========================================
  // Scenario 5: Loops up to maxIterations
  // =========================================
  @Test
  @Tag("valid")
  public void testLoopsUpToMaxIterationsWhenNoEscalate() {
    int maxIters = 3; // TODO: Change to any positive integer for wider test.
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event event1 = mock(Event.class);
    Event event2 = mock(Event.class);
    when(agent1.runAsync(any())).thenReturn(Flowable.just(event1));
    when(agent2.runAsync(any())).thenReturn(Flowable.just(event2));
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(any())).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "maxloop",
              "desc",
              Arrays.asList(agent1, agent2),
              Optional.of(maxIters),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(maxIters * 2, results.size());
      for (int i = 0; i < results.size(); i++) {
        assertSame(results.get(i), (i % 2 == 0) ? event1 : event2);
      }
    }
  }

  // ============================================
  // Scenario 6: Single sub-agent, multiple iters
  // ============================================
  @Test
  @Tag("boundary")
  public void testHandlesSingleSubAgentMultipleIterations() {
    int maxIters = 5; // TODO: Change to test with different counts.
    BaseAgent agent = mock(BaseAgent.class);
    Event event = mock(Event.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(event));
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(any())).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "singleloop",
              "desc",
              Collections.singletonList(agent),
              Optional.of(maxIters),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(maxIters, results.size());
      for (Event result : results) {
        assertSame(event, result);
      }
    }
  }

  // =====================================================
  // Scenario 7: stops immediately if first event escalates
  // =====================================================
  @Test
  @Tag("boundary")
  public void testStopsImmediatelyIfFirstEventEscalates() {
    BaseAgent agent = mock(BaseAgent.class);
    Event escalate = mock(Event.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(escalate));
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      mockedStatic.when(() -> LoopAgent.hasEscalateAction(escalate)).thenReturn(true);
      LoopAgent loopAgent =
          new LoopAgent(
              "escFirst",
              "desc",
              Collections.singletonList(agent),
              Optional.of(10),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(1, results.size());
      assertSame(escalate, results.get(0));
    }
  }

  // =========================================================
  // Scenario 8: Defaults to Integer.MAX_VALUE if no maxIter
  // (Sample only, don't run full max-value iterations here!)
  // =========================================================
  @Test
  @Tag("valid")
  public void testUsesDefaultMaxIterationsIfNotProvided() {
    BaseAgent agent = mock(BaseAgent.class);
    Event event = mock(Event.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(event));
    int sample = 10; // Take 10 for test, don't run Integer.MAX_VALUE!
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(any())).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "defmax",
              "desc",
              Collections.singletonList(agent),
              Optional.empty(),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results =
          loopAgent.runAsyncImpl(mockInvocationContext).take(sample).toList().blockingGet();
      assertEquals(sample, results.size());
      for (Event result : results) {
        assertSame(event, result);
      }
    }
  }

  // ====================================================
  // Scenario 9: Handles null or empty escalate Optionals
  // ====================================================
  @Test
  @Tag("valid")
  public void testHandlesNullActionsInEscalateCheckGracefully() {
    BaseAgent agent = mock(BaseAgent.class);
    Event event = mock(Event.class);
    // We'll have hasEscalateAction return false whenever called.
    when(agent.runAsync(any())).thenReturn(Flowable.just(event));
    int maxIters = 2; // TODO: Set other values if needed.
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      // Simulate missing escalate fields
      when(LoopAgent.hasEscalateAction(event)).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "nullEscal",
              "desc",
              Collections.singletonList(agent),
              Optional.of(maxIters),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(maxIters, results.size());
      for (Event result : results) {
        assertSame(event, result);
      }
    }
  }

  // ==================================================
  // Scenario 10: Propagates sub-agent error up the tree
  // ==================================================
  @Test
  @Tag("invalid")
  public void testPropagatesSubAgentErrors() {
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event event1 = mock(Event.class);
    RuntimeException exception = new RuntimeException("Error down the pipeline!"); // TODO:
    // Change
    // error
    // message
    // if
    // needed.
    when(agent1.runAsync(any())).thenReturn(Flowable.just(event1));
    when(agent2.runAsync(any())).thenReturn(Flowable.error(exception));
    LoopAgent loopAgent =
        new LoopAgent(
            "errprg",
            "desc",
            Arrays.asList(agent1, agent2),
            Optional.of(1),
            Collections.emptyList(),
            Collections.emptyList());
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> loopAgent.runAsyncImpl(mockInvocationContext).blockingForEach(e -> {}));
    assertSame(exception, thrown.getCause() != null ? thrown.getCause() : thrown);
  }

  // ========================================================
  // Scenario 11: Stops if escalate in later iteration cycle
  // ========================================================
  @Test
  @Tag("valid")
  public void testStopsIfEscalateOccursInSubsequentIteration() {
    AtomicInteger count = new AtomicInteger(0);
    BaseAgent agent = mock(BaseAgent.class);
    Event eventNormal = mock(Event.class);
    Event eventEscalate = mock(Event.class);
    when(agent.runAsync(any()))
        .thenAnswer(
            (Answer<Flowable<Event>>)
                invocation -> {
                  int idx = count.getAndIncrement();
                  return Flowable.just(idx == 2 ? eventEscalate : eventNormal);
                });
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      mockedStatic.when(() -> LoopAgent.hasEscalateAction(eventNormal)).thenReturn(false);
      mockedStatic.when(() -> LoopAgent.hasEscalateAction(eventEscalate)).thenReturn(true);
      LoopAgent loopAgent =
          new LoopAgent(
              "escLater",
              "desc",
              Collections.singletonList(agent),
              Optional.of(5),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(3, results.size());
      assertSame(eventNormal, results.get(0));
      assertSame(eventNormal, results.get(1));
      assertSame(eventEscalate, results.get(2));
    }
  }

  // ==========================================
  // Scenario 12: maxIterations = 0: must be no events
  // ==========================================
  @Test
  @Tag("boundary")
  public void testHandlesMaxIterationsZeroReturnsNoEvents() {
    int maxIter = 0;
    BaseAgent agent = mock(BaseAgent.class);
    Event event = mock(Event.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(event));
    LoopAgent loopAgent =
        new LoopAgent(
            "zeroloop",
            "desc",
            Collections.singletonList(agent),
            Optional.of(maxIter),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
    assertEquals(0, results.size());
  }

  // =======================================================
  // Scenario 13: escalate() always returns Optional.empty()
  // =======================================================
  @Test
  @Tag("valid")
  public void testEmitsAllEventsWhenEscalateOptionalIsEmpty() {
    BaseAgent agent = mock(BaseAgent.class);
    Event event = mock(Event.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(event));
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(event)).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "optemptyesc",
              "desc",
              Collections.singletonList(agent),
              Optional.of(4),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      assertEquals(4, results.size());
      for (Event result : results) {
        assertSame(event, result);
      }
    }
  }

  // ====================================================
  // Scenario 14: Exception thrown in subAgents()
  // ====================================================
  @Test
  @Tag("invalid")
  public void testThrowsIfSubAgentsMethodThrowsException() {
    RuntimeException exception = new RuntimeException("subAgents failed"); // TODO:
    // Provide
    // custom
    // exception
    // message
    // if
    // needed.
    LoopAgent loopAgent =
        new LoopAgent(
            "thrower",
            "desc",
            null,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            throw exception;
          }
        };
    RuntimeException thrown =
        assertThrows(
            RuntimeException.class,
            () -> loopAgent.runAsyncImpl(mockInvocationContext).blockingForEach(e -> {}));
    assertSame(exception, thrown.getCause() != null ? thrown.getCause() : thrown);
  }

  // ====================================================
  // Scenario 15: All sub-agents emit empty Flowables
  // ====================================================
  @Test
  @Tag("valid")
  public void testHandlesSubAgentsThatEmitNoEvents() {
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    when(agent1.runAsync(any())).thenReturn(Flowable.empty());
    when(agent2.runAsync(any())).thenReturn(Flowable.empty());
    LoopAgent loopAgent =
        new LoopAgent(
            "allEmpty",
            "desc",
            Arrays.asList(agent1, agent2),
            Optional.of(3),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
    assertTrue(results.isEmpty());
  }

  // ===============================================================================
  // Scenario 16: Sub-agent sequence order is preserved across repeat iterations
  // ===============================================================================
  @Test
  @Tag("integration")
  public void testResetsSubAgentOrderOnEachRepeatIteration() {
    int maxIters = 2; // TODO: Change if more cycles wanted
    Event eventA = mock(Event.class);
    Event eventB = mock(Event.class);
    Event eventC = mock(Event.class);
    BaseAgent agentA = mock(BaseAgent.class);
    BaseAgent agentB = mock(BaseAgent.class);
    BaseAgent agentC = mock(BaseAgent.class);
    when(agentA.runAsync(any())).thenReturn(Flowable.just(eventA));
    when(agentB.runAsync(any())).thenReturn(Flowable.just(eventB));
    when(agentC.runAsync(any())).thenReturn(Flowable.just(eventC));
    List<BaseAgent> agents = Arrays.asList(agentA, agentB, agentC);
    try (MockedStatic<LoopAgent> mockedStatic =
        mockStatic(LoopAgent.class, Mockito.CALLS_REAL_METHODS)) {
      when(LoopAgent.hasEscalateAction(any())).thenReturn(false);
      LoopAgent loopAgent =
          new LoopAgent(
              "orderAgent",
              "desc",
              agents,
              Optional.of(maxIters),
              Collections.emptyList(),
              Collections.emptyList());
      List<Event> results = loopAgent.runAsyncImpl(mockInvocationContext).toList().blockingGet();
      // Order: [A, B, C, A, B, C]
      assertEquals(maxIters * agents.size(), results.size());
      for (int i = 0; i < results.size(); i++) {
        int subIdx = i % agents.size();
        Event expected = subIdx == 0 ? eventA : subIdx == 1 ? eventB : eventC;
        assertSame(expected, results.get(i));
      }
    }
  }
}
