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
import com.google.common.collect.ImmutableList;
import io.reactivex.rxjava3.core.Flowable;
import java.util.*;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;

@ExtendWith(MockitoExtension.class)
public class LoopAgentRunAsyncImplTest {

  @Mock private InvocationContext mockContext;

  // Utility simple Event and BaseAgent definitions for mocking
  static class TestEvent extends Event {

    private final boolean escalate;

    TestEvent(boolean escalate) {
      this.escalate = escalate;
    }

    @Override
    public boolean escalate() {
      return escalate;
    }
  }

  static class DummyBaseAgent extends BaseAgent {

    private final List<Event> eventsToEmit;

    private final Throwable throwable;

    DummyBaseAgent(List<Event> events) {
      this.eventsToEmit = events;
      this.throwable = null;
    }

    DummyBaseAgent(Throwable throwable) {
      this.eventsToEmit = null;
      this.throwable = throwable;
    }

    @Override
    public Flowable<Event> runAsync(InvocationContext context) {
      // Spying invocation context for test 13
      return throwable == null ? Flowable.fromIterable(eventsToEmit) : Flowable.error(throwable);
    }
  }

  // 1. Handling Null Sub-Agents List
  @Test
  @Tag("boundary")
  public void testReturnEmptyFlowableWhenSubAgentsIsNull() {
    LoopAgent agent =
        new LoopAgent("n", "d", null, Optional.empty(), ImmutableList.of(), ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return null;
          }
        };
    assertEquals(0, agent.runAsyncImpl(mockContext).toList().blockingGet().size());
  }

  // 2. Handling Empty Sub-Agents List
  @Test
  @Tag("boundary")
  public void testReturnEmptyFlowableWhenSubAgentsIsEmpty() {
    LoopAgent agent =
        new LoopAgent(
            "n",
            "d",
            ImmutableList.of(),
            Optional.empty(),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of();
          }
        };
    assertTrue(agent.runAsyncImpl(mockContext).toList().blockingGet().isEmpty());
  }

  // 3. Single Sub-Agent with No Escalation and No Max Iterations
  @Test
  @Tag("valid")
  public void testRepeatIndefinitelyWhenNoEscalationAndMaxIterationsIsEmpty() {
    TestEvent evt1 = new TestEvent(false);
    DummyBaseAgent subAgent = new DummyBaseAgent(List.of(evt1));
    LoopAgent agent =
        new LoopAgent(
            "name",
            "desc",
            ImmutableList.of(subAgent),
            Optional.empty(),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(subAgent);
          }
        };
    // Take 10 events for finite assertion as MAX_VALUE is infinite
    List<Event> output = agent.runAsyncImpl(mockContext).take(10).toList().blockingGet();
    assertEquals(10, output.size());
    output.forEach(e -> assertSame(evt1, e));
  }

  // 4. Sub-Agent Returns Event Triggering Escalation
  @Test
  @Tag("valid")
  public void testStopOnEscalationEventFromSubAgent() {
    TestEvent evt1 = new TestEvent(false);
    TestEvent evt2 = new TestEvent(true);
    DummyBaseAgent subAgent = new DummyBaseAgent(List.of(evt1, evt2, new TestEvent(false)));
    LoopAgent agent =
        new LoopAgent(
            "n",
            "d",
            ImmutableList.of(subAgent),
            Optional.of(5),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(subAgent);
          }
        };
    List<Event> result = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(2, result.size());
    assertSame(evt1, result.get(0));
    assertSame(evt2, result.get(1));
  }

  // 5. Multiple Sub-Agents Each with Different Outputs
  @Test
  @Tag("valid")
  public void testConcatenateEventsFromAllSubAgentsInOrder() {
    TestEvent evt1 = new TestEvent(false);
    TestEvent evt2 = new TestEvent(false);
    DummyBaseAgent a1 = new DummyBaseAgent(List.of(evt1));
    DummyBaseAgent a2 = new DummyBaseAgent(List.of(evt2));
    LoopAgent agent =
        new LoopAgent(
            "x",
            "y",
            ImmutableList.of(a1, a2),
            Optional.of(1),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(a1, a2);
          }
        };
    List<Event> result = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(2, result.size());
    assertSame(evt1, result.get(0));
    assertSame(evt2, result.get(1));
  }

  // 6. Maximum Iterations Limit Enforced
  @Test
  @Tag("valid")
  public void testLimitNumberOfLoopsToMaxIterations() {
    TestEvent evt1 = new TestEvent(false);
    DummyBaseAgent agentA = new DummyBaseAgent(List.of(evt1));
    LoopAgent agent =
        new LoopAgent(
            "x",
            "y",
            ImmutableList.of(agentA),
            Optional.of(3),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(agentA);
          }
        };
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(3, events.size());
  }

  // 7. Max Iterations Is Zero
  @Test
  @Tag("boundary")
  public void testReturnEmptyWhenMaxIterationsIsZero() {
    TestEvent evt1 = new TestEvent(false);
    DummyBaseAgent subAgent = new DummyBaseAgent(List.of(evt1));
    LoopAgent agent =
        new LoopAgent(
            "x",
            "y",
            ImmutableList.of(subAgent),
            Optional.of(0),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(subAgent);
          }
        };
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(0, events.size());
  }

  // 8. Sub-Agent Emits Escalation on First Event
  @Test
  @Tag("boundary")
  public void testStopImmediatelyIfFirstEventEscalates() {
    TestEvent escalateEvent = new TestEvent(true);
    DummyBaseAgent sa1 = new DummyBaseAgent(List.of(escalateEvent));
    DummyBaseAgent sa2 = new DummyBaseAgent(List.of(new TestEvent(false))); // Will
    // not be
    // called
    LoopAgent agent =
        new LoopAgent(
            "abc",
            "def",
            ImmutableList.of(sa1, sa2),
            Optional.of(10),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(sa1, sa2);
          }
        };
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(1, events.size());
    assertSame(escalateEvent, events.get(0));
  }

  // 9. Sub-Agent Throws Exception During Emission
  @Test
  @Tag("invalid")
  public void testPropagateErrorWhenSubAgentThrowsException() {
    RuntimeException ex = new RuntimeException("fail!"); // TODO: Replace message as
    // appropriate for your
    // case
    DummyBaseAgent normal = new DummyBaseAgent(List.of(new TestEvent(false)));
    DummyBaseAgent failing = new DummyBaseAgent(ex);
    LoopAgent agent =
        new LoopAgent(
            "xx",
            "yy",
            ImmutableList.of(failing, normal),
            Optional.of(1),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(failing, normal);
          }
        };
    Exception thrown =
        assertThrows(
            Exception.class,
            () -> {
              agent.runAsyncImpl(mockContext).blockingForEach(e -> {});
            });
    assertTrue(thrown.getCause() instanceof RuntimeException);
    assertEquals(ex.getMessage(), thrown.getCause().getMessage());
  }

  // 10. Sub-Agent Emits No Events (Empty Flowable)
  @Test
  @Tag("valid")
  public void testHandleSubAgentWithEmptyEvents() {
    DummyBaseAgent emptyAgent = new DummyBaseAgent(List.of());
    TestEvent evt = new TestEvent(false);
    DummyBaseAgent nonEmptyAgent = new DummyBaseAgent(List.of(evt));
    LoopAgent agent =
        new LoopAgent(
            "n",
            "m",
            ImmutableList.of(emptyAgent, nonEmptyAgent),
            Optional.of(1),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(emptyAgent, nonEmptyAgent);
          }
        };
    List<Event> events = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(1, events.size());
    assertSame(evt, events.get(0));
  }

  // 11. Escalation in Second Iteration
  @Test
  @Tag("valid")
  public void testStopOnEscalationInSubsequentLoop() {
    DummyBaseAgent sa1 = new DummyBaseAgent(List.of(new TestEvent(false)));
    TestEvent normal = new TestEvent(false);
    TestEvent escalate = new TestEvent(true);
    // Will escalate on second iteration
    List<DummyBaseAgent> dynamic =
        Arrays.asList(new DummyBaseAgent(List.of(normal)), new DummyBaseAgent(List.of(escalate)));
    final List<List<DummyBaseAgent>> responses = Arrays.asList(List.of(sa1), dynamic);
    final int[] callCount = {0};
    LoopAgent agent =
        new LoopAgent(
            "test",
            "desc",
            ImmutableList.of(sa1),
            Optional.of(5),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            // Return one agent in the first call, both agents in second
            int c = Math.min(callCount[0]++, responses.size() - 1);
            return responses.get(c);
          }
        };
    List<Event> out = agent.runAsyncImpl(mockContext).toList().blockingGet();
    // Should collect first non-escalate, then escalate and terminate
    assertEquals(2, out.size());
    assertSame(normal, out.get(0));
    assertSame(escalate, out.get(1));
  }

  // 12. Sub-Agents Reference Changes After Construction
  @Test
  @Tag("integration")
  public void testDetectDynamicChangesToSubAgentsAfterConstruction() {
    TestEvent e1 = new TestEvent(false), e2 = new TestEvent(false);
    DummyBaseAgent a1 = new DummyBaseAgent(List.of(e1));
    DummyBaseAgent a2 = new DummyBaseAgent(List.of(e2));
    final List<List<DummyBaseAgent>> dynamicLists = Arrays.asList(List.of(a1), List.of(a2));
    final int[] callIndex = {0};
    LoopAgent agent =
        new LoopAgent(
            "x", "y", dynamicLists.get(0), Optional.of(2), ImmutableList.of(), ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            int idx = Math.min(callIndex[0], dynamicLists.size() - 1);
            return dynamicLists.get(idx++);
          }
        };
    List<Event> events = agent.runAsyncImpl(mockContext).take(2).toList().blockingGet();
    // It should pick e1 first, then e2 due to dynamic change
    assertEquals(2, events.size());
    assertSame(e1, events.get(0));
    assertSame(e2, events.get(1));
  }

  // 13. InvocationContext Is Passed Correctly
  @Test
  @Tag("integration")
  public void testPassThroughInvocationContextToSubAgents() {
    DummyBaseAgent spyAgent = spy(new DummyBaseAgent(List.of(new TestEvent(false))));
    LoopAgent agent =
        new LoopAgent(
            "n",
            "d",
            ImmutableList.of(spyAgent),
            Optional.of(1),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(spyAgent);
          }
        };
    agent.runAsyncImpl(mockContext).toList().blockingGet();
    verify(spyAgent, atLeastOnce()).runAsync((InvocationContext) eq(mockContext));
  }

  // 14. maxIterations is Absent (Optional.empty())
  @Test
  @Tag("boundary")
  public void testUseIntegerMaxValueWhenMaxIterationsIsNotProvided() {
    TestEvent event = new TestEvent(false);
    DummyBaseAgent subAgent = spy(new DummyBaseAgent(List.of(event)));
    LoopAgent agent =
        new LoopAgent(
            "tt",
            "zz",
            ImmutableList.of(subAgent),
            Optional.empty(),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(subAgent);
          }
        };
    // maxIterations empty, should repeat indefinitely; test by taking 5
    List<Event> events = agent.runAsyncImpl(mockContext).take(5).toList().blockingGet();
    assertEquals(5, events.size());
    assertSame(event, events.get(0));
    verify(subAgent, atLeast(2)).runAsync(any());
  }

  // 15. Multiple Escalation Events in Single Iteration
  @Test
  @Tag("valid")
  public void testOnlyStopOnFirstEscalationEvent() {
    TestEvent e1 = new TestEvent(true);
    TestEvent e2 = new TestEvent(true);
    DummyBaseAgent subAgent = new DummyBaseAgent(List.of(e1, e2));
    LoopAgent agent =
        new LoopAgent(
            "p",
            "q",
            ImmutableList.of(subAgent),
            Optional.of(3),
            ImmutableList.of(),
            ImmutableList.of()) {
          @Override
          public List<? extends BaseAgent> subAgents() {
            return ImmutableList.of(subAgent);
          }
        };
    List<Event> out = agent.runAsyncImpl(mockContext).toList().blockingGet();
    assertEquals(1, out.size());
    assertSame(e1, out.get(0));
  }
}
