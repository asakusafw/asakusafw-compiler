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
package com.asakusafw.dag.compiler.directio;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.directio.DirectFileInputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generates {@link DirectFileInputAdapter}.
 * @since 0.4.0
 */
public class DirectFileInputAdapterGenerator {

    /**
     * Generates {@link DirectFileInputAdapter} class.
     * @param context the current context
     * @param input the target input spec
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, Spec input, ClassDescription target) {
        ClassWriter writer = newWriter(target, DirectFileInputAdapter.class);
        defineAdapterConstructor(writer, DirectFileInputAdapter.class, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getConst(v, input.id);
            getConst(v, input.basePath);
            getConst(v, input.resourcePattern);
            getConst(v, input.dataFormat);
            getConst(v, input.dataFilter);
            getConst(v, input.optional);
            v.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    target.getInternalName(), "bind", //$NON-NLS-1$
                    Type.getMethodDescriptor(typeOf(DirectFileInputAdapter.class),
                            typeOf(String.class),
                            typeOf(String.class), typeOf(String.class),
                            typeOf(Class.class), typeOf(Class.class),
                            typeOf(boolean.class)),
                    false);
            v.visitInsn(Opcodes.POP);
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    /**
     * Represents an operation spec for {@link DirectFileInputAdapter}.
     * @since 0.4.0
     */
    public static class Spec {

        final String id;

        final String basePath;

        final String resourcePattern;

        final ClassDescription dataFormat;

        final ClassDescription dataFilter;

        final boolean optional;

        /**
         * Creates a new instance.
         * @param id the input ID
         * @param basePath the base path
         * @param resourcePattern the resource pattern
         * @param dataFormat the data format
         * @param dataFilter the data filter
         * @param optional {@code true} if the input is optional, otherwise {@code false}
         */
        public Spec(
                String id,
                String basePath, String resourcePattern,
                ClassDescription dataFormat, ClassDescription dataFilter,
                boolean optional) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(basePath);
            Arguments.requireNonNull(resourcePattern);
            Arguments.requireNonNull(dataFormat);
            this.id = id;
            this.basePath = basePath;
            this.resourcePattern = resourcePattern;
            this.dataFormat = dataFormat;
            this.dataFilter = dataFilter;
            this.optional = optional;
        }
    }
}
