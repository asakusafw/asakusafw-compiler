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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.io.UnionRecordSerDeSupplier;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * Generates a {@link UnionRecordSerDeSupplier}.
 * @since 0.4.0
 */
public final class UnionRecordSerDeSupplierGenerator {

    private static final String CATEGORY = "jdbc"; //$NON-NLS-1$

    private static final String HINT = "OutputUnionRecordSerDeSupplier"; //$NON-NLS-1$

    private UnionRecordSerDeSupplierGenerator() {
        return;
    }

    /**
     * Creates a {@link UnionRecordSerDeSupplier} class.
     * @param context the current context
     * @param upstreams the upstream specifications
     * @param downstream the downstream specification
     * @return the generated class, or cached one
     */
    public static ClassData generate(ClassGeneratorContext context, List<Upstream> upstreams, Downstream downstream) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(upstreams);
        Arguments.requireNonNull(downstream);
        ClassDescription target = context.getClassName(CATEGORY, HINT);
        return generate(context, upstreams, downstream, target);
    }

    /**
     * Generates {@link UnionRecordSerDeSupplier} class.
     * @param context the current context
     * @param upstreams the upstream specifications
     * @param downstream the downstream specification
     * @param target the target class
     * @return the generated class data
     */
    public static ClassData generate(
            ClassGeneratorContext context,
            List<Upstream> upstreams, Downstream downstream,
            ClassDescription target) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(upstreams);
        Arguments.requireNonNull(downstream);
        ClassWriter writer = newWriter(target, UnionRecordSerDeSupplier.class);
        defineEmptyConstructor(writer, UnionRecordSerDeSupplier.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            self.load(v);
            upstreams.forEach(s -> {
                // this.upstream(tags, elementSerDe)
                getList(v, s.tags);
                getConst(v, s.element);
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        typeOf(UnionRecordSerDeSupplier.class).getInternalName(), "upstream", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(UnionRecordSerDeSupplier.class),
                                typeOf(Collection.class), typeOf(Class.class)),
                        false);
            });
            downstream.tags.forEach(s -> {
                // this.downstream(tag)
                getConst(v, s);
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        typeOf(UnionRecordSerDeSupplier.class).getInternalName(), "downstream", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(UnionRecordSerDeSupplier.class),
                                typeOf(String.class)),
                        false);
            });
            v.visitInsn(Opcodes.POP);
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    /**
     * Specification of upstream ports in {@link UnionRecordSerDeSupplier}.
     * @since 0.4.0
     */
    public static final class Upstream {

        final Set<String> tags;

        final ClassDescription element;

        /**
         * Creates a new instance.
         * @param tags the port tags, never contain nulls
         * @param element the element ser/de class
         */
        public Upstream(Collection<String> tags, ClassDescription element) {
            Arguments.requireNonNull(tags);
            Arguments.require(tags.isEmpty() == false);
            Arguments.requireNonNull(element);
            this.tags = Arguments.freezeToSet(tags);
            this.element = element;
        }

        /**
         * Creates a new instance.
         * @param tags the port tags, never contain nulls
         * @param element the element ser/de class
         * @return the created instance
         */
        public static Upstream of(ClassDescription element, String... tags) {
            return new Upstream(Arrays.asList(tags), element);
        }
    }

    /**
     * Specification of downstream ports in {@link UnionRecordSerDeSupplier}.
     * @since 0.4.0
     */
    public static final class Downstream {

        final Set<String> tags;

        /**
         * Creates a new instance.
         * @param tags the port tags, may contain {@code null}
         */
        public Downstream(Collection<String> tags) {
            Arguments.requireNonNull(tags);
            Arguments.require(tags.isEmpty() == false);
            this.tags = Arguments.freezeToSet(tags);
        }

        /**
         * Creates a new instance.
         * @param tag the port tag (nullable)
         * @return the created instance
         */
        public static Downstream of(String tag) {
            return new Downstream(Collections.singleton(tag));
        }
    }
}
