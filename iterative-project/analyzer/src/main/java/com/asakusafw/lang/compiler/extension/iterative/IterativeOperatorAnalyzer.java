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
package com.asakusafw.lang.compiler.extension.iterative;

import java.text.MessageFormat;
import java.util.Set;

import com.asakusafw.lang.compiler.analyzer.OperatorAttributeAnalyzer;
import com.asakusafw.lang.compiler.analyzer.model.OperatorSource;
import com.asakusafw.lang.compiler.common.Diagnostic.Level;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.util.EnumUtil;
import com.asakusafw.lang.compiler.model.graph.Operator.OperatorKind;
import com.asakusafw.lang.compiler.model.iterative.IterativeExtension;
import com.asakusafw.vocabulary.iterative.Iterative;

/**
 * Analyzes {@link Iterative} annotation.
 * @since 0.3.0
 */
public class IterativeOperatorAnalyzer implements OperatorAttributeAnalyzer {

    private static final Set<OperatorKind> SUPPORTED = EnumUtil.freeze(OperatorKind.INPUT, OperatorKind.USER);

    @Override
    public AttributeMap analyze(OperatorSource source) throws DiagnosticException {
        Iterative annotation = source.getOrigin().getAnnotation(Iterative.class);
        if (annotation == null) {
            return new AttributeMap();
        }
        if (SUPPORTED.contains(source.getOperatorKind()) == false) {
            throw new DiagnosticException(Level.ERROR, MessageFormat.format(
                    "@{0} is not supported: kind={1}, source={2}",
                    Iterative.class.getSimpleName(),
                    source.getOperatorKind(),
                    source.getOrigin()));
        }
        String[] parameterNames = annotation.value();
        return new AttributeMap().put(IterativeExtension.class, new IterativeExtension(parameterNames));
    }
}
