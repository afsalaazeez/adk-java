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
import java.util.*;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

class LoopAgentRunAsyncImplTest {

  private LoopAgent.Builder loopAgentBuilder;

  private InvocationContext mockContext;

  private Callbacks.BeforeAgentCallback beforeAgentCallback;

  private Callbacks.AfterAgentCallback afterAgentCallback;

  @BeforeEach
  void setUp() {
    loopAgentBuilder = mock(LoopAgent.Builder.class, CALLS_REAL_METHODS);
    mockContext = mock(InvocationContext.class);
    beforeAgentCallback = mock(Callbacks.BeforeAgentCallback.class);
    afterAgentCallback = mock(Callbacks.AfterAgentCallback.class);
  }

  // Utility method to create Event with escalate flag
  private static Event escalateEvent() {
    Event escalateEvent = mock(Event.class);
    when(escalateEvent.escalate()).thenReturn(true);
    return escalateEvent;
  }

  private static Event simpleEvent() {
    Event event = mock(Event.class);
    when(event.escalate()).thenReturn(false);
    return event;
  }

  private static class TestLoopAgent extends LoopAgent {

    private List<? extends BaseAgent> subAgentList;

    private Optional<Integer> maxIterationsOpt;

    TestLoopAgent(List<? extends BaseAgent> subAgents, Optional<Integer> maxIterations) {
      super(
          "TestLoopAgent",
          "Testing",
          subAgents,
          maxIterations,
          Collections.emptyList(),
          Collections.emptyList());
      this.subAgentList = subAgents;
      this.maxIterationsOpt = maxIterations;
    }

    @Override
    protected List<? extends BaseAgent> subAgents() {
      return subAgentList;
    }

    @Override
    protected Optional<Integer> maxIterations() {
      return maxIterationsOpt;
    }
  }

  @Test
  @Tag("boundary")
  void testReturnEmptyFlowableWhenSubAgentsIsNull() {
    LoopAgent agent =
        new LoopAgent(
            "Test",
            "desc",
            null,
            Optional.of(1),
            Collections.emptyList(),
            Collections.emptyList()) {
          @Override
          protected List<? extends BaseAgent> subAgents() {
            return null;
          }
        };
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    List<Event> events = result.toList().blockingGet();
    assertTrue((boolean) events.isEmpty());
  }

  @Test
  @Tag("boundary")
  void testReturnEmptyFlowableWhenSubAgentsIsEmpty() {
    LoopAgent agent = new TestLoopAgent(Collections.emptyList(), Optional.of(3));
    Flowable<Event> result = agent.runAsyncImpl(mockContext);
    List<Event> events = result.toList().blockingGet();
    assertTrue((boolean) events.isEmpty());
  }

  @Test
  @Tag("valid")
  void testRepeatIndefinitelyWhenNoEscalationAndMaxIterationsIsEmpty() {
    BaseAgent agentMock = mock(BaseAgent.class);
    Event event = simpleEvent();
    when(agentMock.runAsync(any())).thenReturn(Flowable.just(event));
    LoopAgent agent = new TestLoopAgent(Collections.singletonList(agentMock), Optional.empty());
    // limit to 5 emissions for practical test
    List<Event> list = agent.runAsyncImpl(mockContext).take(5).toList().blockingGet();
    assertEquals((int) 5, (int) list.size());
    assertTrue((boolean) list.stream().allMatch(e -> e == event));
  }

  @Test
  @Tag("valid")
  void testStopOnEscalationEventFromSubAgent() {
    BaseAgent agentMock = mock(BaseAgent.class);
    Event normalEvent = simpleEvent();
    Event escalateEvent = escalateEvent();
    when(agentMock.runAsync(any()))
        .thenReturn(Flowable.just(normalEvent, escalateEvent, simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Collections.singletonList(agentMock), Optional.of(10));
    List<Event> result = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 2, (int) result.size());
    assertSame((Object) normalEvent, (Object) result.get(0));
    assertSame((Object) escalateEvent, (Object) result.get(1));
  }

  @Test
  @Tag("valid")
  void testConcatenateEventsFromAllSubAgentsInOrder() {
    BaseAgent subAgentA = mock(BaseAgent.class);
    BaseAgent subAgentB = mock(BaseAgent.class);
    Event a1 = simpleEvent();
    Event a2 = simpleEvent();
    Event b1 = simpleEvent();
    when(subAgentA.runAsync(any())).thenReturn(Flowable.just(a1, a2));
    when(subAgentB.runAsync(any())).thenReturn(Flowable.just(b1));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(subAgentA, subAgentB), Optional.of(1));
    List<Event> result = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 3, (int) result.size());
    assertSame((Object) a1, (Object) result.get(0));
    assertSame((Object) a2, (Object) result.get(1));
    assertSame((Object) b1, (Object) result.get(2));
  }

  @Test
  @Tag("valid")
  void testLimitNumberOfLoopsToMaxIterations() {
    BaseAgent subAgent = mock(BaseAgent.class);
    Event e1 = simpleEvent();
    when(subAgent.runAsync(any())).thenReturn(Flowable.just(e1));
    int maxIters = 4;
    LoopAgent loopAgent =
        new TestLoopAgent(Collections.singletonList(subAgent), Optional.of(maxIters));
    List<Event> result = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) maxIters, (int) result.size());
  }

  @Test
  @Tag("boundary")
  void testReturnEmptyWhenMaxIterationsIsZero() {
    BaseAgent subAgent = mock(BaseAgent.class);
    when(subAgent.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Collections.singletonList(subAgent), Optional.of(0));
    List<Event> result = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertTrue((boolean) result.isEmpty());
  }

  @Test
  @Tag("boundary")
  void testStopImmediatelyIfFirstEventEscalates() {
    BaseAgent subAgent1 = mock(BaseAgent.class);
    BaseAgent subAgent2 = mock(BaseAgent.class);
    Event escalation = escalateEvent();
    when(subAgent1.runAsync(any())).thenReturn(Flowable.just(escalation));
    // should not be called:
    when(subAgent2.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(subAgent1, subAgent2), Optional.of(3));
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 1, (int) events.size());
    assertSame((Object) escalation, (Object) events.get(0));
    verify(subAgent2, never()).runAsync(any());
  }

  @Test
  @Tag("invalid")
  void testPropagateErrorWhenSubAgentThrowsException() {
    BaseAgent normalAgent = mock(BaseAgent.class);
    BaseAgent errorAgent = mock(BaseAgent.class);
    RuntimeException exception = new RuntimeException("forced error"); // TODO:
    // replace
    // with
    // expected
    // error if
    // required
    when(normalAgent.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    when(errorAgent.runAsync(any())).thenReturn(Flowable.error(exception));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(normalAgent, errorAgent), Optional.of(3));
    List<Event> events = new ArrayList<>();
    Throwable[] caught = new Throwable[1];
    loopAgent.runAsyncImpl(mockContext).subscribe(events::add, err -> caught[0] = err);
    assertNotNull((Object) caught[0]);
    assertEquals((Object) exception, (Object) caught[0]);
    // Only events from normalAgent before error
    assertEquals((int) 1, (int) events.size());
  }

  @Test
  @Tag("valid")
  void testHandleSubAgentWithEmptyEvents() {
    BaseAgent emptyAgent = mock(BaseAgent.class);
    BaseAgent normalAgent = mock(BaseAgent.class);
    Event evt = simpleEvent();
    when(emptyAgent.runAsync(any())).thenReturn(Flowable.empty());
    when(normalAgent.runAsync(any())).thenReturn(Flowable.just(evt));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(emptyAgent, normalAgent), Optional.of(2));
    List<Event> events = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 2, (int) events.size());
    assertTrue((boolean) events.stream().allMatch(e -> e == evt));
  }

  @Test
  @Tag("valid")
  void testStopOnEscalationInSubsequentLoop() {
    BaseAgent agent = mock(BaseAgent.class);
    Event event1 = simpleEvent();
    Event escalate = escalateEvent();
    when(agent.runAsync(any()))
        .thenReturn(Flowable.just(event1))
        .thenReturn(Flowable.just(escalate));
    // simulate two loops, escalate only in second iteration
    LoopAgent loopAgent =
        new LoopAgent(
            "loop",
            "",
            Arrays.asList(agent),
            Optional.of(5),
            Collections.emptyList(),
            Collections.emptyList()) {
          int count = 0;

          @Override
          protected List<? extends BaseAgent> subAgents() {
            return Arrays.asList(
                this.new BaseAgent() {
                  @Override
                  public Flowable<Event> runAsync(InvocationContext context) {
                    return count++ == 0 ? Flowable.just(event1) : Flowable.just(escalate);
                  }
                });
          }
        };
    List<Event> results = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 2, (int) results.size());
    assertSame((Object) event1, (Object) results.get(0));
    assertSame((Object) escalate, (Object) results.get(1));
  }

  @Test
  @Tag("integration")
  void testDetectDynamicChangesToSubAgentsAfterConstruction() {
    final List<BaseAgent> agents1 = new ArrayList<>();
    BaseAgent a1 = mock(BaseAgent.class);
    when(a1.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    agents1.add(a1);
    final List<BaseAgent> agents2 = new ArrayList<>();
    BaseAgent a2 = mock(BaseAgent.class);
    when(a2.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    agents2.add(a2);
    AtomicBoolean switcher = new AtomicBoolean(true);
    LoopAgent loopAgent =
        new LoopAgent(
            "dyn", "", agents1, Optional.of(1), Collections.emptyList(), Collections.emptyList()) {
          @Override
          protected List<? extends BaseAgent> subAgents() {
            return switcher.get() ? agents1 : agents2;
          }
        };
    // First: should pick agents1
    List<Event> first = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 1, (int) first.size());
    // Switch at runtime, next call returns agents2
    switcher.set(false);
    List<Event> second = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 1, (int) second.size());
    // Each agent's runAsync should have been called once
    verify(a1, times(1)).runAsync(any());
    verify(a2, times(1)).runAsync(any());
  }

  @Test
  @Tag("valid")
  void testPassThroughInvocationContextToSubAgents() {
    BaseAgent agent1 = mock(BaseAgent.class);
    BaseAgent agent2 = mock(BaseAgent.class);
    when(agent1.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    when(agent2.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(agent1, agent2), Optional.of(1));
    loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    verify(agent1).runAsync((InvocationContext) eq(mockContext));
    verify(agent2).runAsync((InvocationContext) eq(mockContext));
  }

  @Test
  @Tag("boundary")
  void testUseIntegerMaxValueWhenMaxIterationsIsNotProvided() {
    BaseAgent agent = mock(BaseAgent.class);
    when(agent.runAsync(any())).thenReturn(Flowable.just(simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Collections.singletonList(agent), Optional.empty());
    // We only want to consume 7 events, to verify that it does repeat
    List<Event> result = loopAgent.runAsyncImpl(mockContext).take(7).toList().blockingGet();
    assertEquals((int) 7, (int) result.size());
    verify(agent, atLeast(1)).runAsync(any());
  }

  @Test
  @Tag("invalid")
  void testOnlyStopOnFirstEscalationEvent() {
    BaseAgent agent = mock(BaseAgent.class);
    Event escalate1 = escalateEvent();
    Event escalate2 = escalateEvent();
    when(agent.runAsync(any())).thenReturn(Flowable.just(escalate1, escalate2, simpleEvent()));
    LoopAgent loopAgent = new TestLoopAgent(Arrays.asList(agent), Optional.of(3));
    List<Event> result = loopAgent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals((int) 1, (int) result.size());
    assertSame((Object) escalate1, (Object) result.get(0));
  }
}
