/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.inspection.processor;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.processor.InspectionNodeProcessor.Context;

/**
 * Test for {@link DetailProcessor}.
 */
public class DetailProcessorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InspectionNode node = new InspectionNode("simple", "TESTING");
        process(node);
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
        process(node);
    }

    /**
     * w/ ports.
     */
    @Test
    public void ports() {
        InspectionNode node = new InspectionNode("a", "b");
        node.withInput(new InspectionNode.Port("in"));
        node.withOutput(new InspectionNode.Port("out"));
        process(node);
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
        process(node);
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
        process(node);
    }

    private void process(InspectionNode node) {
        process(context(), node);
    }

    private Context context(String... pairs) {
        assertThat(pairs.length % 2, is(0));
        Map<String, String> map = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            map.put(pairs[i + 0], pairs[i + 1]);
        }
        return new Context(map);
    }

    private void process(Context context, InspectionNode node) {
        DetailProcessor proc = new DetailProcessor();
        System.out.printf("// txt: %s%n", node);
        try {
            proc.process(context, node, System.out);
        } catch (IOException e) {
            throw new AssertionError();
        }
        System.out.println();
    }
}
