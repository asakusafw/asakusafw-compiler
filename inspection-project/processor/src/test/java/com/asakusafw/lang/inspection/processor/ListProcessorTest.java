/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.inspection.processor;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.junit.Test;

import com.asakusafw.lang.inspection.InspectionNode;
import com.asakusafw.lang.inspection.processor.ListProcessor;
import com.asakusafw.lang.inspection.processor.InspectionNodeProcessor.Context;

/**
 * Test for {@link ListProcessor}.
 */
public class ListProcessorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        InspectionNode node = new InspectionNode("root", "ROOT")
            .withElement(new InspectionNode("testing", "TESTING"));
        assertThat(process(node), containsInAnyOrder("testing"));
    }

    /**
     * w/ multiple elements.
     */
    @Test
    public void multiple_elements() {
        InspectionNode node = new InspectionNode("root", "ROOT")
            .withElement(new InspectionNode("a", "AAA"))
            .withElement(new InspectionNode("b", "BBB"))
            .withElement(new InspectionNode("c", "CCC"));
        assertThat(process(node), containsInAnyOrder("a", "b", "c"));
    }

    /**
     * w/o any elements.
     */
    @Test
    public void no_elements() {
        InspectionNode node = new InspectionNode("root", "ROOT");
        assertThat(process(node), hasSize(0));
    }

    private Set<String> process(InspectionNode node) {
        ListProcessor proc = new ListProcessor();
        ByteArrayOutputStream buf = new ByteArrayOutputStream();
        try {
            proc.process(new Context(), node, buf);
        } catch (IOException e) {
            throw new AssertionError();
        }
        Set<String> elements = new HashSet<>();
        try (Scanner scanner = new Scanner(
                new ByteArrayInputStream(buf.toByteArray()),
                ListProcessor.ENCODING.name())) {
            while (scanner.hasNextLine()) {
                String s = scanner.nextLine();
                if (s.isEmpty() == false) {
                    elements.add(s);
                }
            }
        }
        return elements;
    }
}
