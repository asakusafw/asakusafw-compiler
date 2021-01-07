/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.analyzer;

import java.lang.reflect.Type;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.vocabulary.external.ExporterDescription;
import com.asakusafw.vocabulary.external.ImporterDescription;
import com.asakusafw.vocabulary.flow.FlowDescription;
import com.asakusafw.vocabulary.flow.In;
import com.asakusafw.vocabulary.flow.Out;
import com.asakusafw.vocabulary.flow.graph.FlowGraph;
import com.asakusafw.vocabulary.flow.graph.FlowIn;
import com.asakusafw.vocabulary.flow.graph.FlowOut;
import com.asakusafw.vocabulary.flow.graph.InputDescription;
import com.asakusafw.vocabulary.flow.graph.OutputDescription;

/**
 * Analyzes {@link FlowDescription} objects.
 * <h3 id="naming"> input/output naming </h3>
 * <p>
 * Each input/output name must satisfy the following rule:
 * </p>
<pre><code>
Name:
    NameStart NamePart*
NameStart: one of
    A-Z
    a-z
    _
NamePart: one of
    NameStart
    0-9
</code></pre>
 */
public class FlowDescriptionAnalyzer {

    private static final Pattern PATTERN_NAME = Pattern.compile("[A-Za-z_][0-9A-Za-z_]*"); //$NON-NLS-1$

    private final Map<String, FlowIn<?>> inputs = new LinkedHashMap<>();

    private final Map<String, FlowOut<?>> outputs = new LinkedHashMap<>();

    /**
     * Creates a new flow input operator.
     * <p>
     * Each input is unique in the same flow,
     * and its name must satisfy the <em><a href="#naming">naming rule</a></em>.
     * </p>
     * @param <T> input data type
     * @param name the port name
     * @param dataType the port data type
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> In<T> addInput(String name, Type dataType) {
        return add(new InputDescription(name, dataType));
    }

    /**
     * Creates a new external input operator.
     * <p>
     * Each input is unique in the same flow,
     * and its name must satisfy the <em><a href="#naming">naming rule</a></em>.
     * </p>
     * @param <T> input data type
     * @param name the port name
     * @param description the external input description
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> In<T> addInput(String name, ImporterDescription description) {
        return add(new InputDescription(name, description));
    }

    /**
     * Creates a new flow output operator.
     * <p>
     * Each output is unique in the same flow,
     * and its name must satisfy the <em><a href="#naming">naming rule</a></em>.
     * </p>
     * @param <T> output data type
     * @param name the port name
     * @param dataType the port data type
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> Out<T> addOutput(String name, Type dataType) {
        return add(new OutputDescription(name, dataType));
    }

    /**
     * Creates a new external output operator.
     * <p>
     * Each output is unique in the same flow,
     * and its name must satisfy the <em><a href="#naming">naming rule</a></em>.
     * </p>
     * @param <T> output data type
     * @param name the port name
     * @param description the external output description
     * @return the created operator
     * @throws IllegalStateException if the port name is not unique in this flow
     * @throws IllegalArgumentException if the port name is not valid
     */
    public <T> Out<T> addOutput(String name, ExporterDescription description) {
        return add(new OutputDescription(name, description));
    }

    private <T> In<T> add(InputDescription description) {
        String name = description.getName();
        if (isValidName(name) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "input name must be valid identifier: {0}",
                    name));
        }
        if (inputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "input name must be unique: {0}",
                    name));
        }
        FlowIn<T> operator = new FlowIn<>(description);
        inputs.put(name, operator);
        return operator;
    }

    private <T> Out<T> add(OutputDescription description) {
        String name = description.getName();
        if (isValidName(name) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "output name must be valid identifier: {0}",
                    name));
        }
        if (outputs.containsKey(name)) {
            throw new IllegalStateException(MessageFormat.format(
                    "output name must be unique: {0}",
                    name));
        }
        FlowOut<T> output = new FlowOut<>(description);
        outputs.put(name, output);
        return output;
    }

    private static boolean isValidName(String name) {
        if (name == null) {
            return false;
        }
        return PATTERN_NAME.matcher(name).matches();
    }

    /**
     * Analyze {@link FlowDescription} object using previously added flow inputs/outputs.
     * @param description the target flow description
     * @return the analyzed flow graph
     * @throws DiagnosticException if failed to analyze flow DSL
     */
    public FlowGraph analyze(FlowDescription description) {
        try {
            description.start();
        } catch (Exception e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "error occurred while executing {0}#describe()",
                    description.getClass().getName()), e);
        }
        FlowGraph result = new FlowGraph(
                description.getClass(),
                new ArrayList<>(inputs.values()),
                new ArrayList<>(outputs.values()));
        inputs.clear();
        outputs.clear();
        return result;
    }
}
