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
package com.asakusafw.dag.compiler.internalio;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.List;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.internalio.InternalOutputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generates {@link InternalOutputAdapter}.
 * @since 0.4.0
 */
public class InternalOutputAdapterGenerator {

    /**
     * Generates {@link InternalOutputAdapter} class.
     * @param context the current context
     * @param outputs the target output specs
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, List<Spec> outputs, ClassDescription target) {
        ClassWriter writer = newWriter(target, InternalOutputAdapter.class);
        defineAdapterConstructor(writer, InternalOutputAdapter.class, v -> {
            for (Spec spec : outputs) {
                v.visitVarInsn(Opcodes.ALOAD, 0);
                getConst(v, spec.id);
                getConst(v, spec.path);
                getConst(v, spec.dataType);
                v.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        target.getInternalName(), "bind", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(InternalOutputAdapter.class),
                                typeOf(String.class), typeOf(String.class), typeOf(Class.class)),
                        false);
                v.visitInsn(Opcodes.POP);
            }
        });
        return new ClassData(target, writer::toByteArray);
    }

    /**
     * Represents an output spec for internal I/O.
     * @since 0.4.0
     */
    public static class Spec {

        final String id;

        final String path;

        final TypeDescription dataType;

        /**
         * Creates a new instance.
         * @param id the output ID
         * @param path the output path
         * @param dataType the output data type
         */
        public Spec(String id, String path, TypeDescription dataType) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(path);
            Arguments.requireNonNull(dataType);
            this.id = id;
            this.path = path;
            this.dataType = dataType;
        }
    }
}
