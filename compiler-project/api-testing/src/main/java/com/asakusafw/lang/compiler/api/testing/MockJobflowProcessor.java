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
package com.asakusafw.lang.compiler.api.testing;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.asakusafw.lang.compiler.api.Exclusive;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.api.reference.ExternalInputReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.ExternalInput;
import com.asakusafw.lang.compiler.model.graph.ExternalOutput;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.graph.Operators;

/**
 * A mock implementation of {@link JobflowProcessor}.
 * <p>
 * This only accepts {@link ExternalInput} and {@link ExternalOutput} operators,
 * and this simply registers their external I/O operators to the processor context.
 * </p>
 * <p>
 * This may be used for testing {@link ExternalPortProcessor}s.
 * </p>
 * @see MockExternalPortProcessor
 */
@Exclusive(optional = true)
public class MockJobflowProcessor implements JobflowProcessor {

    @Override
    public void process(Context context, Jobflow source) throws IOException {
        List<ExternalInput> inputs = new ArrayList<>();
        List<ExternalOutput> outputs = new ArrayList<>();
        for (Operator operator : source.getOperatorGraph().getOperators()) {
            switch (operator.getOperatorKind()) {
            case INPUT:
                inputs.add((ExternalInput) operator);
                break;
            case OUTPUT:
                outputs.add((ExternalOutput) operator);
                break;
            default:
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "{0} only accepts external input/output operators: {1}",
                        MockJobflowProcessor.class.getSimpleName(),
                        operator));
            }
        }
        Map<ExternalInput, ExternalInputReference> resolved = new LinkedHashMap<>();
        for (ExternalInput input : inputs) {
            ExternalInputReference reference = context.addExternalInput(input.getName(), input.getInfo());
            resolved.put(input, reference);
        }
        for (ExternalOutput output : outputs) {
            Set<String> paths = new LinkedHashSet<>();
            for (Operator pred : Operators.getPredecessors(output)) {
                assert pred.getOperatorKind() == OperatorKind.INPUT;
                ExternalInputReference upstream = resolved.get(pred);
                assert upstream != null;
                paths.addAll(upstream.getPaths());
            }
            context.addExternalOutput(output.getName(), output.getInfo(), paths);
        }
    }
}
