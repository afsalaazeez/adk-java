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
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.subscribers.TestSubscriber;
import java.util.Collections;
import java.util.Optional;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class LoopAgentRunLiveImplTest {

  @Test
  @Tag("invalid")
  public void testUnsupportedOperationExceptionOnRunLiveImpl() {
    // Arrange
    String name = "TestAgent";
    String description = "Desc";
    // TODO: fill with actual BaseAgent implementations if needed
    java.util.List<? extends BaseAgent> subAgents = Collections.emptyList();
    Optional<Integer> maxIterations = Optional.of(5);
    java.util.List<Callbacks.BeforeAgentCallback> beforeAgentCallbacks = Collections.emptyList();
    java.util.List<Callbacks.AfterAgentCallback> afterAgentCallbacks = Collections.emptyList();
    // Create LoopAgent instance
    LoopAgent loopAgent =
        new LoopAgent(
            name, description, subAgents, maxIterations, beforeAgentCallbacks, afterAgentCallbacks);
    // Create a mock or dummy InvocationContext
    InvocationContext invocationContext = Mockito.mock(InvocationContext.class);
    // Act
    Flowable<Event> result = loopAgent.runLiveImpl(invocationContext);
    // Assert
    TestSubscriber<Event> subscriber = new TestSubscriber<>();
    result.subscribe(subscriber);
    subscriber.assertError(
        throwable ->
            throwable instanceof UnsupportedOperationException
                && "runLive is not defined for LoopAgent yet.".equals(throwable.getMessage()));
  }
}
