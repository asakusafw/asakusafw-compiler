/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.inspection.json;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.InspectionNodeRepository;

/**
 * Test for {@link JsonInspectionNodeRepository}.
 */
public class JsonInspectionNodeRepositoryTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InspectionNode node = new InspectionNode("a", "b");
        test(node);
    }

    /**
     * w/ properties.
     */
    @Test
    public void properties() {
        InspectionNode node = new InspectionNode("a", "b");
        node.withProperty("a", "100");
        node.withProperty("b", "200");
        node.withProperty("c", "300");
        test(node);
    }

    /**
     * w/ ports.
     */
    @Test
    public void ports() {
        InspectionNode node = new InspectionNode("a", "b");
        node.withInput(new InspectionNode.Port("in"));
        node.withOutput(new InspectionNode.Port("out"));
        test(node);
    }

    /**
     * w/ ports.
     */
    @Test
    public void ports_properties() {
        InspectionNode node = new InspectionNode("a", "b");
        node.withInput(new InspectionNode.Port("in")
            .withProperty("a", "100"));
        node.withOutput(new InspectionNode.Port("out")
            .withProperty("b", "200"));
        test(node);
    }

    /**
     * w/ ports.
     */
    @Test
    public void ports_opposites() {
        InspectionNode node = new InspectionNode("a", "b");
        node.withInput(new InspectionNode.Port("in")
            .withOpposite(new InspectionNode.PortReference("a", "out")));
        node.withOutput(new InspectionNode.Port("out")
            .withOpposite(new InspectionNode.PortReference("a", "in")));
        test(node);
    }

    /**
     * w/ elements.
     */
    @Test
    public void elements() {
        InspectionNode node = new InspectionNode("a", "A");
        node.withElement(new InspectionNode("b", "B"));
        node.withElement(new InspectionNode("c", "C"));
        node.withElement(new InspectionNode("d", "D"));
        test(node);
    }

    /**
     * load from invalid input.
     * @throws Exception must occur
     */
    @Test(expected = IOException.class)
    public void load_invalid() throws Exception {
        try (InputStream in = new ByteArrayInputStream(new byte[] { 0x00 })) {
            new JsonInspectionNodeRepository().load(in);
        }
    }

    /**
     * load from invalid input.
     * @throws Exception must occur
     */
    @Test(expected = IOException.class)
    public void load_empty() throws Exception {
        try (InputStream in = new ByteArrayInputStream(new byte[0])) {
            new JsonInspectionNodeRepository().load(in);
        }
    }

    /**
     * store to erroneous output.
     * @throws Exception must occur
     */
    @Test(expected = IOException.class)
    public void store_error() throws Exception {
        try (OutputStream out = new OutputStream() {
            @Override
            public void write(int b) throws IOException {
                throw new IOException("testing");
            }
        }) {
            new JsonInspectionNodeRepository().store(out, new InspectionNode("a", "A"));
        }
    }

    private void test(InspectionNode node) {
        testNode(node, restore(node));
    }

    private void testNodes(Map<String, InspectionNode> expected, Map<String, InspectionNode> actual) {
        assertThat(actual.keySet(), is(expected.keySet()));
        for (InspectionNode element : expected.values()) {
            InspectionNode target = actual.get(element.getId());
            assertThat(target, is(notNullValue()));
            testNode(element, target);
        }
    }

    private void testNode(InspectionNode expected, InspectionNode actual) {
        assertThat(actual.toString(), actual.getId(), is(expected.getId()));
        assertThat(actual.getTitle(), is(expected.getTitle()));
        assertThat(actual.getProperties(), is(expected.getProperties()));

        testPorts(expected.getInputs(), actual.getInputs());
        testPorts(expected.getOutputs(), actual.getOutputs());
        testNodes(expected.getElements(), actual.getElements());
    }

    private void testPorts(Map<String, InspectionNode.Port> expected, Map<String, InspectionNode.Port> actual) {
        assertThat(actual.keySet(), is(expected.keySet()));
        for (InspectionNode.Port element : expected.values()) {
            InspectionNode.Port target = actual.get(element.getId());
            assertThat(target, is(notNullValue()));
            testPort(element, target);
        }
    }

    private void testPort(InspectionNode.Port expected, InspectionNode.Port actual) {
        assertThat(actual.toString(), actual.getId(), is(expected.getId()));
        assertThat(actual.getProperties(), is(expected.getProperties()));
        assertThat(actual.getOpposites(), is(expected.getOpposites()));
    }

    private InspectionNode restore(InspectionNode node) {
        try {
            InspectionNodeRepository repo = new JsonInspectionNodeRepository();
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            repo.store(output, node);
            ByteArrayInputStream input = new ByteArrayInputStream(output.toByteArray());
            return repo.load(input);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }
}
