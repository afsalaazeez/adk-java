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
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.util.*;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class LoopAgentRunAsyncImplTest {

  private InvocationContext mockContext;

  @BeforeEach
  public void setUp() {
    mockContext = mock(InvocationContext.class);
  }

  static class TestLoopAgent extends LoopAgent {

    private final List<? extends BaseAgent> testSubAgents;

    TestLoopAgent(
        String name,
        String description,
        List<? extends BaseAgent> subAgents,
        Optional<Integer> maxIterations,
        List<Callbacks.BeforeAgentCallback> beforeAgentCallback,
        List<Callbacks.AfterAgentCallback> afterAgentCallback) {
      super(name, description, subAgents, maxIterations, beforeAgentCallback, afterAgentCallback);
      this.testSubAgents = subAgents;
    }

    @Override
    protected List<? extends BaseAgent> subAgents() {
      return testSubAgents;
    }
  }

  // region Helper/base setup
  interface InvocationContext {}

  // Minimal BaseAgent stub for mocking
  abstract static class BaseAgent {

    public Flowable<Event> runAsync(InvocationContext invocationContext) {
      return Flowable.empty();
    }
  }

  // Minimal Event stub for mocking/escalate logic
  static class TestEvent extends Event {

    private final List<Action> actions;

    TestEvent(List<Action> actions) {
      this.actions = actions;
    }

    @Override
    public List<Action> actions() {
      return actions;
    }
  }

  interface Action {

    Optional<Boolean> escalate();
  }

  static class TestAction implements Action {

    private final Optional<Boolean> escalate;

    TestAction(Optional<Boolean> escalate) {
      this.escalate = escalate;
    }

    @Override
    public Optional<Boolean> escalate() {
      return escalate;
    }
  }

  // region Test methods
  @Test
  @Tag("boundary")
  public void returnsEmptyFlowableWhenSubAgentsIsNull() {
    LoopAgent agent =
        new LoopAgent(
            "name",
            "desc",
            null,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          protected List<? extends BaseAgent> subAgents() {
            return null;
          }
        };
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    TestSubscriber<Event> subscriber = new TestSubscriber<>();
    result.subscribe(subscriber);
    subscriber.assertNoValues();
    subscriber.assertComplete();
  }

  @Test
  @Tag("boundary")
  public void returnsEmptyFlowableWhenSubAgentsIsEmpty() {
    List<BaseAgent> subAgents = Collections.emptyList();
    LoopAgent agent =
        new TestLoopAgent(
            "name",
            "desc",
            subAgents,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList());
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    TestSubscriber<Event> subscriber = new TestSubscriber<>();
    result.subscribe(subscriber);
    subscriber.assertNoValues();
    subscriber.assertComplete();
  }

  @Test
  @Tag("valid")
  public void executesAllSubAgentsOnceIfNoMaxIterationsAndNoEscalate() {
    // loop will repeat Integer.MAX_VALUE, only take e.g. first 6 events
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event evt1 = new TestEvent(Arrays.asList(new TestAction(Optional.of(false))));
    Event evt2 = new TestEvent(Arrays.asList(new TestAction(Optional.of(false))));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(evt1));
    when(agent2.runAsync(mockContext)).thenReturn(Flowable.just(evt2));
    List<BaseAgent> subs = Arrays.asList(agent1, agent2);
    LoopAgent agent =
        new TestLoopAgent(
            "name",
            "desc",
            subs,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList());
    Flowable<Event> result = agent.runAsyncImpl(mockContext).take(6);
    List<Event> events = result.toList().blockingGet();
    for (int i = 0; i < events.size(); i += 2) {
      assertSame(evt1, events.get(i), "Should alternate first event from agent1");
      assertSame(evt2, events.get(i + 1), "Should alternate second event from agent2");
    }
    assertEquals(6, events.size(), "Should produce 6 events from 3 cycles of 2 sub-agents");
  }

  @Test
  @Tag("valid")
  public void stopsIterationWhenSubAgentEscalates() {
    // second agent, on first cycle, escalate=true
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event evt1 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    Event evt2 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(true))));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(evt1));
    when(agent2.runAsync(mockContext)).thenReturn(Flowable.just(evt2));
    List<BaseAgent> subs = Arrays.asList(agent1, agent2);
    LoopAgent agent =
        new TestLoopAgent(
            "name",
            "desc",
            subs,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList());
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    List<Event> events = result.toList().blockingGet();
    assertEquals(2, events.size(), "Should emit both agent events before escalate halts iteration");
    assertSame(evt1, events.get(0));
    assertSame(evt2, events.get(1));
  }

  @Test
  @Tag("valid")
  public void loopsUpToMaxIterationsWhenNoEscalate() {
    int maxIter = 3;
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    Event evt1 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    Event evt2 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(evt1));
    when(agent2.runAsync(mockContext)).thenReturn(Flowable.just(evt2));
    List<BaseAgent> subs = Arrays.asList(agent1, agent2);
    LoopAgent agent =
        new TestLoopAgent(
            "name",
            "desc",
            subs,
            Optional.of(maxIter),
            Collections.emptyList(),
            Collections.emptyList());
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    List<Event> events = result.toList().blockingGet();
    assertEquals(
        2 * maxIter,
        events.size(),
        "Should run all sub-agents exactly maxIterations times in order");
    for (int i = 0; i < maxIter; i++) {
      assertSame(evt1, events.get(i * 2));
      assertSame(evt2, events.get(i * 2 + 1));
    }
  }

  @Test
  @Tag("valid")
  public void handlesSingleSubAgentMultipleIterations() {
    int maxIter = 5;
    BaseAgent agent = mock(BaseAgent.class);
    Event event = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    when(agent.runAsync(mockContext)).thenReturn(Flowable.just(event));
    List<BaseAgent> subs = Collections.singletonList(agent);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name",
            "desc",
            subs,
            Optional.of(maxIter),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(maxIter, events.size(), "Should emit one event per iteration");
    for (Event evt : events) {
      assertSame(event, evt);
    }
  }

  @Test
  @Tag("boundary")
  public void stopsImmediatelyIfFirstEventEscalates() {
    BaseAgent agent1 = mock(BaseAgent.class);
    Event escalateEvent =
        new TestEvent(Collections.singletonList(new TestAction(Optional.of(true))));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(escalateEvent));
    List<BaseAgent> subs = Collections.singletonList(agent1);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(5), Collections.emptyList(), Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(1, events.size(), "Should emit only one event if escalate first event");
    assertSame(escalateEvent, events.get(0));
  }

  @Test
  @Tag("boundary")
  public void usesDefaultMaxIterationsIfNotProvided() {
    BaseAgent agent = mock(BaseAgent.class);
    Event evt = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    when(agent.runAsync(mockContext)).thenReturn(Flowable.just(evt));
    int cycles = 10;
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name",
            "desc",
            Collections.singletonList(agent),
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).take(cycles).toList().blockingGet();
    assertEquals(
        cycles, events.size(), "Should default to Integer.MAX_VALUE, but test up to sample cycles");
    for (Event e : events) assertSame(evt, e);
  }

  @Test
  @Tag("valid")
  public void handlesNullActionsInEscalateCheckGracefully() {
    BaseAgent agent1 = mock(BaseAgent.class);
    // actions() returns null
    Event evt1 = mock(Event.class);
    when(evt1.actions()).thenReturn(null);
    BaseAgent agent2 = mock(BaseAgent.class);
    // escalate() returns Optional.empty()
    Event evt2 = new TestEvent(Collections.singletonList(new TestAction(Optional.empty())));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(evt1));
    when(agent2.runAsync(mockContext)).thenReturn(Flowable.just(evt2));
    List<BaseAgent> subs = Arrays.asList(agent1, agent2);
    int iterations = 3;
    LoopAgent agent =
        new TestLoopAgent(
            "name",
            "desc",
            subs,
            Optional.of(iterations),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(
        iterations * 2,
        events.size(),
        "Should emit all events for all iterations despite null actions/escalate empty");
    for (int i = 0; i < events.size(); i += 2) {
      assertSame(evt1, events.get(i));
      assertSame(evt2, events.get(i + 1));
    }
  }

  @Test
  @Tag("invalid")
  public void propagatesSubAgentErrors() {
    BaseAgent agent = mock(BaseAgent.class);
    RuntimeException ex = new RuntimeException("Test error"); // TODO update error
    // message if needed
    when(agent.runAsync(mockContext)).thenReturn(Flowable.error(ex));
    List<BaseAgent> subs = Collections.singletonList(agent);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(1), Collections.emptyList(), Collections.emptyList());
    Flowable<Event> result = loopAgent.runAsyncImpl(mockContext);
    TestSubscriber<Event> subscriber = new TestSubscriber<>();
    result.subscribe(subscriber);
    subscriber.assertError(ex);
  }

  @Test
  @Tag("valid")
  public void stopsIfEscalateOccursInSubsequentIteration() {
    BaseAgent agent1 = mock(BaseAgent.class);
    Event evt1 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    Event evt2 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(true))));
    // First run: no escalate, second run: escalate
    when(agent1.runAsync(mockContext))
        .thenReturn(
            Flowable.just(evt1), // first
            // iteration
            Flowable.just(evt2) // second iteration
            );
    List<BaseAgent> subs = Collections.singletonList(agent1);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(5), Collections.emptyList(), Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(
        2,
        events.size(),
        "Should emit one event per iteration until escalate is seen in second iteration");
    assertSame(evt1, events.get(0));
    assertSame(evt2, events.get(1));
  }

  @Test
  @Tag("boundary")
  public void handlesMaxIterationsZeroReturnsNoEvents() {
    BaseAgent agent1 = mock(BaseAgent.class);
    Event evt1 = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.just(evt1));
    List<BaseAgent> subs = Collections.singletonList(agent1);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(0), Collections.emptyList(), Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(0, events.size(), "When maxIterations=0, no events should be emitted");
  }

  @Test
  @Tag("valid")
  public void emitsAllEventsWhenEscalateOptionalIsEmpty() {
    int maxIter = 4;
    BaseAgent agent = mock(BaseAgent.class);
    Event evt = new TestEvent(Collections.singletonList(new TestAction(Optional.empty())));
    when(agent.runAsync(mockContext)).thenReturn(Flowable.just(evt));
    List<BaseAgent> agents = Collections.singletonList(agent);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name",
            "desc",
            agents,
            Optional.of(maxIter),
            Collections.emptyList(),
            Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(maxIter, events.size(), "Should emit all events, escalate always empty");
    for (Event e : events) assertSame(evt, e);
  }

  @Test
  @Tag("invalid")
  public void throwsIfSubAgentsMethodThrowsException() {
    LoopAgent throwing =
        new LoopAgent(
            "name",
            "desc",
            null,
            Optional.empty(),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          protected List<? extends BaseAgent> subAgents() {
            throw new RuntimeException("Oops");
          }
        };
    Flowable<Event> result = throwing.runAsyncImpl(mockContext);
    TestSubscriber<Event> subscriber = new TestSubscriber<>();
    result.subscribe(subscriber);
    subscriber.assertError(RuntimeException.class);
    // Optionally assert error msg:
    // assertTrue(subscriber.errors().get(0).getMessage().contains("Oops"));
  }

  @Test
  @Tag("boundary")
  public void handlesSubAgentsThatEmitNoEvents() {
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    when(agent1.runAsync(mockContext)).thenReturn(Flowable.empty());
    when(agent2.runAsync(mockContext)).thenReturn(Flowable.empty());
    List<BaseAgent> subs = Arrays.asList(agent1, agent2);
    LoopAgent loopAgent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(2), Collections.emptyList(), Collections.emptyList());
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(0, events.size(), "If all subagents emit empty, overall result is empty");
  }

  @Test
  @Tag("valid")
  public void resetsSubAgentOrderOnEachRepeatIteration() {
    BaseAgent a = mock(BaseAgent.class, "A");
    BaseAgent b = mock(BaseAgent.class, "B");
    BaseAgent c = mock(BaseAgent.class, "C");
    Event evA = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    Event evB = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    Event evC = new TestEvent(Collections.singletonList(new TestAction(Optional.of(false))));
    when(a.runAsync(mockContext)).thenReturn(Flowable.just(evA));
    when(b.runAsync(mockContext)).thenReturn(Flowable.just(evB));
    when(c.runAsync(mockContext)).thenReturn(Flowable.just(evC));
    List<BaseAgent> subs = Arrays.asList(a, b, c);
    LoopAgent agent =
        new TestLoopAgent(
            "name", "desc", subs, Optional.of(2), Collections.emptyList(), Collections.emptyList());
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(6, events.size(), "2 iterations * 3 agents each = 6 events");
    // Should be [A, B, C, A, B, C]
    assertSame(evA, events.get(0));
    assertSame(evB, events.get(1));
    assertSame(evC, events.get(2));
    assertSame(evA, events.get(3));
    assertSame(evB, events.get(4));
    assertSame(evC, events.get(5));
  }
}
