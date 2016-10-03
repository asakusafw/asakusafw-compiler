/**
 * Copyright 2011-2016 Asakusa Framework Team.
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

import java.util.List;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.skeleton.EdgeOutputAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generates {@link EdgeOutputAdapter}.
 * @since 0.4.0
 */
public class EdgeOutputAdapterGenerator {

    /**
     * Generates {@link EdgeOutputAdapter} class.
     * @param context the current context
     * @param specs the target output ports
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, List<Spec> specs, ClassDescription target) {
        ClassWriter writer = AsmUtil.newWriter(target, EdgeOutputAdapter.class);
        defineAdapterConstructor(writer, EdgeOutputAdapter.class, v -> {
            for (Spec spec : specs) {
                v.visitVarInsn(Opcodes.ALOAD, 0);
                getConst(v, spec.id);
                getConst(v, spec.mapper);
                getConst(v, spec.copier);
                getConst(v, spec.combiner);
                v.visitMethodInsn(
                        Opcodes.INVOKEVIRTUAL,
                        target.getInternalName(), "bind",
                        Type.getMethodDescriptor(typeOf(EdgeOutputAdapter.class),
                                typeOf(String.class),
                                typeOf(Class.class),
                                typeOf(Class.class), typeOf(Class.class)),
                        false);
                v.visitInsn(Opcodes.POP);
            }
        });
        return new ClassData(target, writer::toByteArray);
    }

    /**
     * Represents an output spec.
     * @since 0.4.0
     */
    public static class Spec {

        final String id;

        final TypeDescription dataType;

        final ClassDescription mapper;

        final ClassDescription copier;

        final ClassDescription combiner;

        /**
         * Creates a new instance.
         * @param id the output ID
         * @param dataType the output data type
         */
        public Spec(String id, TypeDescription dataType) {
            this(id, dataType, null, null, null);
        }

        /**
         * Creates a new instance.
         * @param id the output ID
         * @param dataType the output data type
         * @param mapper the mapper
         */
        public Spec(String id, TypeDescription dataType, ClassDescription mapper) {
            this(id, dataType, mapper, null, null);
        }

        /**
         * Creates a new instance.
         * @param id the output ID
         * @param dataType the output data type
         * @param mapper the mapper
         * @param copier the copier
         * @param combiner the combiner
         */
        public Spec(String id, TypeDescription dataType,
                ClassDescription mapper,
                ClassDescription copier,
                ClassDescription combiner) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(dataType);
            this.id = id;
            this.dataType = dataType;
            this.mapper = mapper;
            this.copier = copier;
            this.combiner = combiner;
        }
    }
}
