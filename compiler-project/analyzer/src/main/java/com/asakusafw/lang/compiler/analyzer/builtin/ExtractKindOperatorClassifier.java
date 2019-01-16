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
package com.asakusafw.lang.compiler.analyzer.builtin;

import java.util.List;

import com.asakusafw.lang.compiler.model.graph.Operator;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorInput.InputUnit;
import com.asakusafw.lang.compiler.optimizer.OperatorCharacterizer;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputAttribute;
import com.asakusafw.lang.compiler.optimizer.basic.OperatorClass.InputType;

/**
 * Provides {@link OperatorClass} for generic <em>extract kind</em> operators.
 */
public class ExtractKindOperatorClassifier implements OperatorCharacterizer<OperatorClass> {

    @Override
    public OperatorClass extract(Context context, Operator operator) {
        List<OperatorInput> inputs = operator.getInputs();
        if (inputs.isEmpty()) {
            throw new IllegalArgumentException();
        }
        int count = 0;
        OperatorClass.Builder builder = OperatorClass.builder(operator, InputType.RECORD);
        for (OperatorInput input : inputs) {
            InputUnit unit = input.getInputUnit();
            if (unit == InputUnit.GROUP) {
                throw new IllegalStateException();
            } else if (unit == InputUnit.WHOLE) {
                continue;
            }
            builder.with(input, InputAttribute.PRIMARY);
            count++;
        }
        if (count != 1) {
            throw new IllegalStateException();
        }
        return Util.resolve(builder, operator);
    }
}
