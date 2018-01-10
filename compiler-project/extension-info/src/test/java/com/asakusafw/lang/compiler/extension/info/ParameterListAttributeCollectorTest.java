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
package com.asakusafw.lang.compiler.extension.info;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.regex.Pattern;

import org.junit.Test;

import com.asakusafw.info.ParameterInfo;
import com.asakusafw.info.ParameterListAttribute;
import com.asakusafw.lang.compiler.info.MockAttributeCollectorContext;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.info.BatchInfo;

/**
 * Test for {@link ParameterListAttributeCollector}.
 */
public class ParameterListAttributeCollectorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        Batch model = new Batch(
                "testing",
                new ClassDescription("com.example.ExampleBatch"),
                null,
                Arrays.asList(new BatchInfo.Parameter[] {
                        new BatchInfo.Parameter(
                                "a",
                                null,
                                false,
                                null),
                        new BatchInfo.Parameter(
                                "b",
                                "parameter comment",
                                true,
                                Pattern.compile(".+")),
                }),
                EnumSet.of(BatchInfo.Attribute.STRICT_PARAMETERS));
        ParameterListAttribute info = new ParameterListAttribute(
                Arrays.asList(new ParameterInfo[] {
                        new ParameterInfo("a", null, false, null),
                        new ParameterInfo("b", "parameter comment", true, ".+"),
                }),
                true);
        assertThat(
                new MockAttributeCollectorContext().collect(model, new ParameterListAttributeCollector()),
                contains(info));
    }
}
