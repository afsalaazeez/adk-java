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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import com.google.adk.agents.BaseAgent;
import java.util.NoSuchElementException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.*;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Arrays.stream;
import static java.util.function.Function.identity;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import javax.annotation.Nonnull;

class AgentStaticLoaderLoadAgentTest {

	private BaseAgent mockAgent1;

	private BaseAgent mockAgent2;

	private BaseAgent mockAgent3;

	private AgentStaticLoader loader;

	@BeforeEach
	void setUp() {
		mockAgent1 = mock(BaseAgent.class);
		mockAgent2 = mock(BaseAgent.class);
		mockAgent3 = mock(BaseAgent.class);
	}

	@Test
    @Tag("valid")
    void loadAgentWithValidName() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        BaseAgent result = loader.loadAgent("testAgent");
        assertNotNull(result);
        assertSame(mockAgent1, result);
    }

	@Test
    @Tag("invalid")
    void loadAgentWithNullName() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> loader.loadAgent(null)
        );
        assertEquals("Agent name cannot be null or empty", exception.getMessage());
    }

	@Test
    @Tag("invalid")
    void loadAgentWithEmptyString() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> loader.loadAgent("")
        );
        assertEquals("Agent name cannot be null or empty", exception.getMessage());
    }

	@Test
    @Tag("boundary")
    void loadAgentWithWhitespaceOnlyName() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> loader.loadAgent("   ")
        );
        assertEquals("Agent name cannot be null or empty", exception.getMessage());
    }

	@Test
    @Tag("invalid")
    void loadAgentWithNonExistentName() {
        when(mockAgent1.name()).thenReturn("agent1");
        when(mockAgent2.name()).thenReturn("agent2");
        loader = new AgentStaticLoader(mockAgent1, mockAgent2);
        NoSuchElementException exception = assertThrows(
            NoSuchElementException.class,
            () -> loader.loadAgent("nonExistentAgent")
        );
        assertTrue(exception.getMessage().contains("Agent not found: nonExistentAgent"));
    }

	@Test
    @Tag("boundary")
    void loadAgentWithLeadingWhitespace() {
        when(mockAgent1.name()).thenReturn("validAgent");
        loader = new AgentStaticLoader(mockAgent1);
        NoSuchElementException exception = assertThrows(
            NoSuchElementException.class,
            () -> loader.loadAgent(" validAgent")
        );
        assertTrue(exception.getMessage().contains("Agent not found:  validAgent"));
    }

	@Test
    @Tag("boundary")
    void loadAgentWithTrailingWhitespace() {
        when(mockAgent1.name()).thenReturn("validAgent");
        loader = new AgentStaticLoader(mockAgent1);
        NoSuchElementException exception = assertThrows(
            NoSuchElementException.class,
            () -> loader.loadAgent("validAgent ")
        );
        assertTrue(exception.getMessage().contains("Agent not found: validAgent "));
    }

	@Test
    @Tag("valid")
    void loadMultipleDifferentAgents() {
        when(mockAgent1.name()).thenReturn("agent1");
        when(mockAgent2.name()).thenReturn("agent2");
        when(mockAgent3.name()).thenReturn("agent3");
        loader = new AgentStaticLoader(mockAgent1, mockAgent2, mockAgent3);
        BaseAgent result1 = loader.loadAgent("agent1");
        BaseAgent result2 = loader.loadAgent("agent2");
        BaseAgent result3 = loader.loadAgent("agent3");
        assertSame(mockAgent1, result1);
        assertSame(mockAgent2, result2);
        assertSame(mockAgent3, result3);
    }

	@Test
    @Tag("valid")
    void loadSameAgentMultipleTimes() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        BaseAgent result1 = loader.loadAgent("testAgent");
        BaseAgent result2 = loader.loadAgent("testAgent");
        BaseAgent result3 = loader.loadAgent("testAgent");
        assertSame(result1, result2);
        assertSame(result2, result3);
        assertSame(mockAgent1, result1);
    }

	@Test
    @Tag("boundary")
    void loadAgentWithCaseSensitiveName() {
        when(mockAgent1.name()).thenReturn("TestAgent");
        loader = new AgentStaticLoader(mockAgent1);
        NoSuchElementException exception = assertThrows(
            NoSuchElementException.class,
            () -> loader.loadAgent("testagent")
        );
        assertTrue(exception.getMessage().contains("Agent not found: testagent"));
    }

	@Test
	@Tag("boundary")
	void loadAgentFromEmptyLoader() {
		loader = new AgentStaticLoader();
		NoSuchElementException exception = assertThrows(NoSuchElementException.class,
				() -> loader.loadAgent("anyAgent"));
		assertTrue(exception.getMessage().contains("Agent not found: anyAgent"));
	}

	@Test
	@Tag("valid")
	void loadAgentWithSpecialCharactersInName() {
		BaseAgent agent1 = mock(BaseAgent.class);
		BaseAgent agent2 = mock(BaseAgent.class);
		BaseAgent agent3 = mock(BaseAgent.class);
		BaseAgent agent4 = mock(BaseAgent.class);
		when(agent1.name()).thenReturn("agent-1");
		when(agent2.name()).thenReturn("agent_2");
		when(agent3.name()).thenReturn("agent.3");
		when(agent4.name()).thenReturn("agent@4");
		loader = new AgentStaticLoader(agent1, agent2, agent3, agent4);
		BaseAgent result1 = loader.loadAgent("agent-1");
		BaseAgent result2 = loader.loadAgent("agent_2");
		BaseAgent result3 = loader.loadAgent("agent.3");
		BaseAgent result4 = loader.loadAgent("agent@4");
		assertSame(agent1, result1);
		assertSame(agent2, result2);
		assertSame(agent3, result3);
		assertSame(agent4, result4);
	}

	@Test
	@Tag("boundary")
	void loadAgentWithVeryLongName() {
		String longName = "a".repeat(1000);
		when(mockAgent1.name()).thenReturn(longName);
		loader = new AgentStaticLoader(mockAgent1);
		BaseAgent result = loader.loadAgent(longName);
		assertNotNull(result);
		assertSame(mockAgent1, result);
	}

	@Test
	@Tag("valid")
	void loadAgentWithUnicodeCharactersInName() {
		BaseAgent agent1 = mock(BaseAgent.class);
		BaseAgent agent2 = mock(BaseAgent.class);
		BaseAgent agent3 = mock(BaseAgent.class);
		BaseAgent agent4 = mock(BaseAgent.class);
		when(agent1.name()).thenReturn("агент");
		when(agent2.name()).thenReturn("代理");
		when(agent3.name()).thenReturn("وكيل");
		when(agent4.name()).thenReturn("🤖agent");
		loader = new AgentStaticLoader(agent1, agent2, agent3, agent4);
		BaseAgent result1 = loader.loadAgent("агент");
		BaseAgent result2 = loader.loadAgent("代理");
		BaseAgent result3 = loader.loadAgent("وكيل");
		BaseAgent result4 = loader.loadAgent("🤖agent");
		assertSame(agent1, result1);
		assertSame(agent2, result2);
		assertSame(agent3, result3);
		assertSame(agent4, result4);
	}

	@Test
    @Tag("invalid")
    void verifyExceptionMessageForNullName() {
        when(mockAgent1.name()).thenReturn("testAgent");
        loader = new AgentStaticLoader(mockAgent1);
        IllegalArgumentException exception = assertThrows(
            IllegalArgumentException.class,
            () -> loader.loadAgent(null)
        );
        assertEquals("Agent name cannot be null or empty", exception.getMessage());
    }

	@Test
    @Tag("invalid")
    void verifyExceptionMessageForNonExistentAgent() {
        when(mockAgent1.name()).thenReturn("existingAgent");
        loader = new AgentStaticLoader(mockAgent1);
        NoSuchElementException exception = assertThrows(
            NoSuchElementException.class,
            () -> loader.loadAgent("missingAgent")
        );
        assertEquals("Agent not found: missingAgent", exception.getMessage());
    }

	@Test
    @Tag("valid")
    void loadAgentAfterLoaderInitialization() {
        when(mockAgent1.name()).thenReturn("immediateAgent");
        loader = new AgentStaticLoader(mockAgent1);
        BaseAgent result = loader.loadAgent("immediateAgent");
        assertNotNull(result);
        assertSame(mockAgent1, result);
    }

	@Test
    @Tag("valid")
    void loadAgentWithExactNameMatch() {
        when(mockAgent1.name()).thenReturn("ExactAgentName");
        loader = new AgentStaticLoader(mockAgent1);
        BaseAgent result = loader.loadAgent("ExactAgentName");
        assertNotNull(result);
        assertSame(mockAgent1, result);
    }

	@Test
    @Tag("valid")
    void verifyNoSideEffectsOnRepeatedCalls() {
        when(mockAgent1.name()).thenReturn("repeatAgent");
        loader = new AgentStaticLoader(mockAgent1);
        BaseAgent result1 = loader.loadAgent("repeatAgent");
        BaseAgent result2 = loader.loadAgent("repeatAgent");
        BaseAgent result3 = loader.loadAgent("repeatAgent");
        BaseAgent result4 = loader.loadAgent("repeatAgent");
        BaseAgent result5 = loader.loadAgent("repeatAgent");
        BaseAgent result6 = loader.loadAgent("repeatAgent");
        BaseAgent result7 = loader.loadAgent("repeatAgent");
        BaseAgent result8 = loader.loadAgent("repeatAgent");
        BaseAgent result9 = loader.loadAgent("repeatAgent");
        BaseAgent result10 = loader.loadAgent("repeatAgent");
        assertSame(mockAgent1, result1);
        assertSame(result1, result2);
        assertSame(result2, result3);
        assertSame(result3, result4);
        assertSame(result4, result5);
        assertSame(result5, result6);
        assertSame(result6, result7);
        assertSame(result7, result8);
        assertSame(result8, result9);
        assertSame(result9, result10);
    }

	@Test
    @Tag("boundary")
    void loadAgentWithSingleCharacterName() {
        when(mockAgent1.name()).thenReturn("A");
        when(mockAgent2.name()).thenReturn("1");
        loader = new AgentStaticLoader(mockAgent1, mockAgent2);
        BaseAgent result1 = loader.loadAgent("A");
        BaseAgent result2 = loader.loadAgent("1");
        assertNotNull(result1);
        assertNotNull(result2);
        assertSame(mockAgent1, result1);
        assertSame(mockAgent2, result2);
    }

}