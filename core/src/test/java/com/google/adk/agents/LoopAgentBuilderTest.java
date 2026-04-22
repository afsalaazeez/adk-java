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

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class LoopAgentBuilderTest {

  @Test
  @Tag("valid")
  @DisplayName("Builder Instantiation Returns Non-Null Builder")
  public void testBuilderInstantiationReturnsNonNullBuilder() {
    LoopAgent.Builder builder = LoopAgent.builder();
    assertNotNull(builder, "Builder instance should not be null");
  }

  @Test
  @Tag("valid")
  @DisplayName("Builder Instance Is Of Correct Type")
  public void testBuilderReturnsCorrectType() {
    Object builder = LoopAgent.builder();
    assertTrue(
        builder instanceof LoopAgent.Builder, "Builder should be instance of LoopAgent.Builder");
  }

  @Test
  @Tag("valid")
  @DisplayName("Builder Is Independently Mutable")
  public void testBuilderInstancesAreIndependent() {
    LoopAgent.Builder builder1 = LoopAgent.builder();
    LoopAgent.Builder builder2 = LoopAgent.builder();
    builder1.name("FirstAgent"); // TODO: Change name as desired
    builder2.name("SecondAgent"); // TODO: Change name as desired
    assertNotSame(builder1, builder2, "Each builder() call should create a new Builder instance");
    // Use their string representations for state diff (no getters)
    assertNotEquals(
        builder1.toString(), builder2.toString(), "Builder state should differ after mutation");
  }

  @Test
  @Tag("boundary")
  @DisplayName("Builder Default State Has No Max Iterations")
  public void testBuilderDefaultHasEmptyMaxIterations() {
    LoopAgent.Builder builder = LoopAgent.builder();
    Optional<Integer> maxIterations = builder.maxIterations;
    assertEquals(Optional.empty(), maxIterations, "Default maxIterations should be Optional.empty");
  }

  @Test
  @Tag("valid")
  @DisplayName("Builder Method CanBeChained")
  public void testBuilderSupportsMethodChaining() {
    LoopAgent.Builder builder = LoopAgent.builder();
    LoopAgent.Builder result =
        builder
            .name("ChainAgent") // TODO: Change value as
            // needed
            .description("Testing chained methods") // TODO: Change value as needed
            .maxIterations(10); // TODO: Change value as needed
    assertSame(builder, result, "Method chaining should return the same Builder instance");
    assertEquals(
        "ChainAgent", ((LoopAgent.Builder) result).name, "Chained name() should set field");
    assertEquals(
        "Testing chained methods",
        ((LoopAgent.Builder) result).description,
        "Chained description() should set field");
    assertEquals(
        Optional.of(10),
        ((LoopAgent.Builder) result).maxIterations,
        "Chained maxIterations() should set field");
  }

  @Test
  @Tag("integration")
  @DisplayName("Builder Is Thread Safe For Instantiation")
  public void testBuilderInstantiationIsThreadSafe() throws Exception {
    final int THREAD_COUNT = 10;
    final List<LoopAgent.Builder> builders = new CopyOnWriteArrayList<>();
    Thread[] threads = new Thread[THREAD_COUNT];
    Exception[] exceptions = new Exception[THREAD_COUNT];
    for (int i = 0; i < THREAD_COUNT; i++) {
      final int index = i;
      threads[i] =
          new Thread(
              () -> {
                try {
                  builders.add(LoopAgent.builder());
                } catch (Exception ex) {
                  exceptions[index] = ex;
                }
              });
    }
    for (Thread thread : threads) thread.start();
    for (Thread thread : threads) thread.join();
    assertEquals(THREAD_COUNT, builders.size(), "Expected builder instances equal to thread count");
    for (int i = 0; i < THREAD_COUNT; i++) {
      assertNull(exceptions[i], "No exceptions should be thrown during builder creation");
    }
    for (int i = 0; i < builders.size(); i++) {
      for (int j = i + 1; j < builders.size(); j++) {
        assertNotSame(
            builders.get(i), builders.get(j), "Each builder should be independent across threads");
      }
    }
  }

  @Test
  @Tag("boundary")
  @DisplayName("Builder Can Be Used Without Providing Any Parameters")
  public void testBuilderBuildsWithNoParameters() {
    LoopAgent.Builder builder = LoopAgent.builder();
    LoopAgent agent = builder.build();
    assertNotNull(agent, "LoopAgent should not be null when built with no configuration");
    assertEquals(
        Optional.empty(), agent.maxIterations, "maxIterations should be empty for default build");
  }

  @Test
  @Tag("valid")
  @DisplayName("Builder Initialization Does Not Throw Exception")
  public void testBuilderInitializationIsExceptionFree() {
    assertDoesNotThrow(
        () -> {
          LoopAgent.Builder builder = LoopAgent.builder();
          assertNotNull(builder, "Builder instance should not be null");
        });
  }
}
