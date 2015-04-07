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
import com.asakusafw.lang.compiler.inspection.InspectionNode.Port;
import com.asakusafw.lang.compiler.inspection.InspectionNode.PortReference;
import com.asakusafw.lang.compiler.inspection.processor.InspectionNodeProcessor.Context;

/**
 * Test for {@link DotProcessor}.
 */
public class DotProcessorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InspectionNode root = new InspectionNode("simple", "TESTING");
        root.withElement(new InspectionNode("a", "AAA")
                .withInput(new Port("in"))
                .withOutput(new Port("out").withOpposite(new PortReference("b", "in"))));
        root.withElement(new InspectionNode("b", "BBB")
                .withInput(new Port("in").withOpposite(new PortReference("a", "out")))
                .withOutput(new Port("out").withOpposite(new PortReference("c", "in"))));
        root.withElement(new InspectionNode("c", "CCC")
                .withInput(new Port("in").withOpposite(new PortReference("b", "out")))
                .withOutput(new Port("out")));
        process(root);
    }

    /**
     * show properties.
     */
    @Test
    public void properties() {
        InspectionNode root = new InspectionNode("properties", "TESTING");
        root.withElement(new InspectionNode("a", "AAA")
                .withInput(new Port("in"))
                .withOutput(new Port("out").withOpposite(new PortReference("b", "in")))
                .withProperty("message", "Hello, world!"));
        root.withElement(new InspectionNode("b", "BBB")
                .withInput(new Port("in").withOpposite(new PortReference("a", "out")))
                .withOutput(new Port("out"))
                .withProperty("x", "XXX")
                .withProperty("y", "YYY")
                .withProperty("z", "ZZZ"));
        process(context(DotProcessor.KEY_VERBOSE, "true"), root);
    }

    /**
     * empty.
     */
    @Test
    public void no_elements() {
        InspectionNode root = new InspectionNode("empty", "TESTING");
        process(root);
    }

    /**
     * diamond.
     */
    @Test
    public void diamond() {
        InspectionNode root = new InspectionNode("diamond", "TESTING");
        root.withElement(new InspectionNode("a", "AAA")
                .withInput(new Port("i0"))
                .withOutput(new Port("o0").withOpposite(new PortReference("b", "i0")))
                .withOutput(new Port("o1").withOpposite(new PortReference("c", "i0"))));
        root.withElement(new InspectionNode("b", "BBB")
                .withInput(new Port("i0").withOpposite(new PortReference("a", "o0")))
                .withOutput(new Port("o0").withOpposite(new PortReference("d", "i0"))));
        root.withElement(new InspectionNode("c", "CCC")
                .withInput(new Port("i0").withOpposite(new PortReference("a", "o1")))
                .withOutput(new Port("o0").withOpposite(new PortReference("d", "i1"))));
        root.withElement(new InspectionNode("d", "DDD")
                .withInput(new Port("i0").withOpposite(new PortReference("b", "o0")))
                .withInput(new Port("i1").withOpposite(new PortReference("c", "o0")))
                .withOutput(new Port("o1")));
        process(root);
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
        DotProcessor proc = new DotProcessor();
        System.out.printf("// dot: %s%n", node);
        try {
            proc.process(context, node, System.out);
        } catch (IOException e) {
            throw new AssertionError();
        }
        System.out.println();
    }
}
