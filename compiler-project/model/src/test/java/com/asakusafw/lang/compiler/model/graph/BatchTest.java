/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.model.graph;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Collections;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Test for {@link Batch}.
 */
public class BatchTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Batch b0 = new Batch(
                "b0",
                new ClassDescription("B0"),
                null,
                Collections.emptyList(),
                Collections.emptyList());

        assertThat(b0.toString(), b0.getBatchId(), is("b0"));
        assertThat(b0.toString(), b0.getDescriptionClass().getClassName(), is("B0"));
        assertThat(b0.toString(), b0.getComment(), is(nullValue()));
        assertThat(b0.toString(), b0.getParameters(), hasSize(0));
        assertThat(b0.toString(), b0.getAttributes(), hasSize(0));

        Jobflow f0 = new Jobflow("f0", new ClassDescription("F0"), new OperatorGraph());
        BatchElement e0 = b0.addElement(f0);
        assertThat(e0.toString(), e0.getJobflow(), is(f0));
        assertThat(b0.getElements(), containsInAnyOrder(e0));
        assertThat(b0.findElement(f0), is(e0));

        Jobflow f1 = new Jobflow("f1", new ClassDescription("F1"), new OperatorGraph());
        BatchElement e1 = b0.addElement(f1);
        assertThat(e1.toString(), e1.getJobflow(), is(f1));
        assertThat(b0.getElements(), containsInAnyOrder(e0, e1));
        assertThat(b0.findElement(f1), is(e1));

        e1.addBlockerElement(e0);
        assertThat(e0.getBlockerElements(), is(empty()));
        assertThat(e0.getBlockingElements(), containsInAnyOrder(e1));
        assertThat(e1.getBlockerElements(), containsInAnyOrder(e0));
        assertThat(e1.getBlockingElements(), is(empty()));
    }
}
