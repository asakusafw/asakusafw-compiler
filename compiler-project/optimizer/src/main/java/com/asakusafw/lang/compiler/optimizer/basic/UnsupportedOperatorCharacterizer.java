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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacteristics;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;

/**
 * Just raises {@link DiagnosticException}.
 * @param <T> the characteristics type
 */
public class UnsupportedOperatorCharacterizer<T extends OperatorCharacteristics>
        implements OperatorCharacterizer<T> {

    @Override
    public T extract(OperatorCharacterizer.Context context, Operator operator) {
        throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                "unsupported to characterize operator: {0}",
                operator));
    }
}
