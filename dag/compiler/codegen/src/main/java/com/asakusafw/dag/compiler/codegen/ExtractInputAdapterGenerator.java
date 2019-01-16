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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.skeleton.ExtractInputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generates {@link ExtractInputAdapter}.
 * @since 0.4.0
 */
public class ExtractInputAdapterGenerator {

    /**
     * Generates {@link ExtractInputAdapter} class.
     * @param context the current context
     * @param input the target input spec
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, Spec input, ClassDescription target) {
        ClassWriter writer = newWriter(target, ExtractInputAdapter.class);
        defineAdapterConstructor(writer, ExtractInputAdapter.class, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getConst(v, input.id);
            v.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    target.getInternalName(), "bind", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(ExtractInputAdapter.class), typeOf(String.class)),
                    false);
            v.visitInsn(Opcodes.POP);
        });
        return new ClassData(target, writer::toByteArray);
    }

    /**
     * Represents an input spec for extract-kind vertices.
     * @since 0.4.0
     */
    public static class Spec {

        final String id;

        final TypeDescription dataType;

        /**
         * Creates a new instance.
         * @param id the input ID
         * @param dataType the input data type
         */
        public Spec(String id, TypeDescription dataType) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(dataType);
            this.id = id;
            this.dataType = dataType;
        }
    }
}
