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
package com.google.adk.web;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.google.adk.agents.BaseAgent;
import com.google.adk.web.config.AgentLoadingProperties;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Field;
import java.util.NoSuchElementException;
import java.util.function.Supplier;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class CompiledAgentLoaderLoadAgentTest {

	@Mock
	private AgentLoadingProperties properties;

	@Mock
	private BaseAgent mockAgent1;

	@Mock
	private BaseAgent mockAgent2;

	private CompiledAgentLoader loader;

	@BeforeEach
  void setUp() {
    when(properties.getSourceDir()).thenReturn("");
    loader = new CompiledAgentLoader(properties);
  }

	private void injectAgentSuppliers(ImmutableMap<String, Supplier<BaseAgent>> suppliers) throws Exception {
		Field field = CompiledAgentLoader.class.getDeclaredField("agentSuppliers");
		field.setAccessible(true);
		field.set(loader, suppliers);
	}

	@Test
	@Tag("valid")
	void loadAgentWithValidName() throws Exception {
		Supplier<BaseAgent> supplier = () -> mockAgent1;
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", supplier);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("testAgent");
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("invalid")
	void loadAgentWithNullName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
			loader.loadAgent(null);
		});
		assertTrue(exception.getMessage().contains("Agent name cannot be null or empty"));
	}

	@Test
	@Tag("invalid")
	void loadAgentWithEmptyName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
			loader.loadAgent("");
		});
		assertTrue(exception.getMessage().contains("Agent name cannot be null or empty"));
	}

	@Test
	@Tag("boundary")
	void loadAgentWithWhitespaceOnlyName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
			loader.loadAgent("   ");
		});
		assertTrue(exception.getMessage().contains("Agent name cannot be null or empty"));
	}

	@Test
	@Tag("invalid")
	void loadAgentWithNonExistentName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("agent1", () -> mockAgent1, "agent2",
				() -> mockAgent2);
		injectAgentSuppliers(agentMap);
		NoSuchElementException exception = assertThrows(NoSuchElementException.class, () -> {
			loader.loadAgent("nonExistentAgent");
		});
		assertTrue(exception.getMessage().contains("Agent not found: nonExistentAgent"));
	}

	@Test
	@Tag("invalid")
	void loadAgentWhenSupplierThrowsException() throws Exception {
		RuntimeException originalException = new RuntimeException("Supplier failed");
		Supplier<BaseAgent> failingSupplier = () -> {
			throw originalException;
		};
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("failingAgent", failingSupplier);
		injectAgentSuppliers(agentMap);
		IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
			loader.loadAgent("failingAgent");
		});
		assertTrue(exception.getMessage().contains("Agent exists but failed to load: failingAgent"));
		assertSame(originalException, exception.getCause());
	}

	@Test
	@Tag("valid")
	void loadAgentMultipleTimesReturnsSameInstance() throws Exception {
		Supplier<BaseAgent> memoizedSupplier = Suppliers.memoize(() -> mockAgent1);
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("cachedAgent", memoizedSupplier);
		injectAgentSuppliers(agentMap);
		BaseAgent result1 = loader.loadAgent("cachedAgent");
		BaseAgent result2 = loader.loadAgent("cachedAgent");
		assertNotNull(result1);
		assertNotNull(result2);
		assertSame(result1, result2);
	}

	@Test
	@Tag("valid")
	void loadDifferentAgentsReturnsDifferentInstances() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("agent1", () -> mockAgent1, "agent2",
				() -> mockAgent2);
		injectAgentSuppliers(agentMap);
		BaseAgent result1 = loader.loadAgent("agent1");
		BaseAgent result2 = loader.loadAgent("agent2");
		assertNotNull(result1);
		assertNotNull(result2);
		assertNotSame(result1, result2);
	}

	@Test
	@Tag("boundary")
	void loadAgentWithCaseSensitiveName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("TestAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		NoSuchElementException exception = assertThrows(NoSuchElementException.class, () -> {
			loader.loadAgent("testagent");
		});
		assertTrue(exception.getMessage().contains("Agent not found"));
	}

	@Test
	@Tag("boundary")
	void loadAgentWhenSupplierReturnsNull() throws Exception {
		Supplier<BaseAgent> nullSupplier = () -> null;
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("nullAgent", nullSupplier);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("nullAgent");
		assertNull(result);
	}

	@Test
	@Tag("valid")
	void loadAgentWithSpecialCharactersInName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("my-chat-agent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("my-chat-agent");
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("boundary")
	void loadAgentWhenAgentSuppliersMapIsEmpty() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> emptyMap = ImmutableMap.of();
		injectAgentSuppliers(emptyMap);
		NoSuchElementException exception = assertThrows(NoSuchElementException.class, () -> {
			loader.loadAgent("anyAgent");
		});
		assertTrue(exception.getMessage().contains("Agent not found"));
	}

	@Test
	@Tag("valid")
	void loadAgentWithUnderscoreAndDotInName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("code_assistant.v2", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("code_assistant.v2");
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("boundary")
	void loadAgentWithTabsInName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
			loader.loadAgent("\t\t");
		});
		assertTrue(exception.getMessage().contains("Agent name cannot be null or empty"));
	}

	@Test
	@Tag("boundary")
	void loadAgentWithMixedWhitespace() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("testAgent", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		IllegalArgumentException exception = assertThrows(IllegalArgumentException.class, () -> {
			loader.loadAgent(" \t \n ");
		});
		assertTrue(exception.getMessage().contains("Agent name cannot be null or empty"));
	}

	@Test
	@Tag("invalid")
	void loadAgentWhenSupplierThrowsNullPointerException() throws Exception {
		NullPointerException originalException = new NullPointerException("NPE in supplier");
		Supplier<BaseAgent> failingSupplier = () -> {
			throw originalException;
		};
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("npeAgent", failingSupplier);
		injectAgentSuppliers(agentMap);
		IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
			loader.loadAgent("npeAgent");
		});
		assertTrue(exception.getMessage().contains("Agent exists but failed to load: npeAgent"));
		assertSame(originalException, exception.getCause());
	}

	@Test
	@Tag("valid")
	void loadAgentWithLongName() throws Exception {
		String longName = "a".repeat(100);
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of(longName, () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent(longName);
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("valid")
	void loadAgentWithNumericName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("12345", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("12345");
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("boundary")
	void loadAgentWithSingleCharacterName() throws Exception {
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("a", () -> mockAgent1);
		injectAgentSuppliers(agentMap);
		BaseAgent result = loader.loadAgent("a");
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("invalid")
	void loadAgentWhenSupplierThrowsIllegalStateException() throws Exception {
		IllegalStateException originalException = new IllegalStateException("Invalid state");
		Supplier<BaseAgent> failingSupplier = () -> {
			throw originalException;
		};
		ImmutableMap<String, Supplier<BaseAgent>> agentMap = ImmutableMap.of("stateAgent", failingSupplier);
		injectAgentSuppliers(agentMap);
		IllegalStateException exception = assertThrows(IllegalStateException.class, () -> {
			loader.loadAgent("stateAgent");
		});
		assertTrue(exception.getMessage().contains("Agent exists but failed to load: stateAgent"));
		assertSame(originalException, exception.getCause());
	}

}
