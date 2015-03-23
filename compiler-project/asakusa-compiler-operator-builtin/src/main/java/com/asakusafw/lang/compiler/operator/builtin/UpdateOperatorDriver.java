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
package com.asakusafw.lang.compiler.operator.builtin;

import javax.lang.model.element.Modifier;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.operator.AbstractOperatorDriver;
import com.asakusafw.lang.compiler.operator.Constants;
import com.asakusafw.lang.compiler.operator.OperatorDriver;
import com.asakusafw.lang.compiler.operator.builtin.DslBuilder.ElementRef;
import com.asakusafw.lang.compiler.operator.builtin.DslBuilder.TypeRef;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription;
import com.asakusafw.lang.compiler.operator.model.OperatorDescription.Document;

/**
 * {@link OperatorDriver} for {@code Update} annotation.
 */
public class UpdateOperatorDriver extends AbstractOperatorDriver {

    private static final String OUTPUT_PORT = "outputPort"; //$NON-NLS-1$

    @Override
    public ClassDescription getAnnotationTypeName() {
        return Constants.getBuiltinOperatorClass("Update"); //$NON-NLS-1$
    }

    @Override
    public OperatorDescription analyze(Context context) {
        DslBuilder dsl = new DslBuilder(context);
        if (dsl.method().modifiers().contains(Modifier.ABSTRACT)) {
            dsl.method().error("Update operator method must not be \"abstract\"");
        }
        if (dsl.result().type().isVoid() == false) {
            dsl.method().error("Update operator method must return \"void\"");
        }
        for (ElementRef p : dsl.parameters()) {
            TypeRef type = p.type();
            if (type.isDataModel()) {
                if (dsl.getInputs().isEmpty()) {
                    dsl.addInput(p.document(), p.name(), p.type().mirror(), p.reference());
                    dsl.addOutput(
                            Document.text("updated dataset"),
                            dsl.annotation().string(OUTPUT_PORT),
                            p.type().mirror(),
                            p.reference());
                } else {
                    p.error("This operator must not have multiple data model type parameters");
                }
            } else if (type.isBasic()) {
                dsl.consumeArgument(p);
            } else {
                p.error("This operator's parameters must be either data model type or basic type");
            }
        }
        return dsl.toDescription();
    }
}
