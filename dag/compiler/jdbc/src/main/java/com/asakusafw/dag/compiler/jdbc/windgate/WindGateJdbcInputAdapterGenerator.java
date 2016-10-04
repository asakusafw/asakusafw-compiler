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
package com.asakusafw.dag.compiler.jdbc.windgate;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.ClassGeneratorContext;
import com.asakusafw.dag.compiler.codegen.SupplierGenerator;
import com.asakusafw.dag.compiler.jdbc.ResultSetAdapterGenerator;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.jdbc.operation.JdbcInputAdapter;
import com.asakusafw.dag.runtime.jdbc.util.WindGateJdbcDirect;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.lang.utils.common.Tuple;

/**
 * Generates {@link JdbcInputAdapter} using {@link WindGateJdbcDirect} API.
 * @since 0.4.0
 */
public final class WindGateJdbcInputAdapterGenerator {

    private static final Type TYPE_BUILDER = typeOf(WindGateJdbcDirect.InputBuilder.class);

    private static final String CATEGORY = "jdbc.windgate"; //$NON-NLS-1$

    private static final String HINT = "InputAdapter"; //$NON-NLS-1$

    private WindGateJdbcInputAdapterGenerator() {
        return;
    }

    /**
     * Generates {@link JdbcInputAdapter} class.
     * @param context the current context
     * @param spec the target input spec
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, Spec spec) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(spec);
        return generate(context, Collections.singletonList(spec));
    }

    /**
     * Generates {@link JdbcInputAdapter} class.
     * @param context the current context
     * @param specs the target input specs
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, List<Spec> specs) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(specs);
        return generate(context, specs, context.getClassName(CATEGORY, HINT));
    }

    /**
     * Generates {@link JdbcInputAdapter} class.
     * @param context the current context
     * @param spec the target input spec
     * @param target the target class
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, Spec spec, ClassDescription target) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(spec);
        return generate(context, Collections.singletonList(spec), target);
    }

    /**
     * Generates {@link JdbcInputAdapter} class.
     * @param context the current context
     * @param specs the target input specs
     * @param target the target class
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, List<Spec> specs, ClassDescription target) {
        Arguments.requireNonNull(context);
        Arguments.requireNonNull(specs);
        ClassWriter writer = newWriter(target, JdbcInputAdapter.class);
        defineAdapterConstructor(writer, JdbcInputAdapter.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            for (Spec spec : specs) {
                self.load(v);
                getConst(v, spec.id);
                getConst(v, spec.model.getProfileName());

                getConst(v, spec.model.getProfileName());
                getConst(v, spec.model.getTableName());
                getList(v, Lang.project(spec.model.getColumnMappings(), Tuple::left));
                getNew(v, getAdapter(context, spec));
                v.visitMethodInsn(Opcodes.INVOKESTATIC,
                        typeOf(WindGateJdbcDirect.class).getInternalName(),
                        "input",
                        Type.getMethodDescriptor(TYPE_BUILDER,
                                typeOf(String.class), // profileName
                                typeOf(String.class), // tableName
                                typeOf(List.class), // columnNames
                                typeOf(Supplier.class)), // adapter
                        false);
                Lang.forEach(Optionals.of(spec.model.getCondition()), s -> {
                    getConst(v, s);
                    v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            TYPE_BUILDER.getInternalName(), "withCondition", //$NON-NLS-1$
                            Type.getMethodDescriptor(TYPE_BUILDER, typeOf(String.class)),
                            false);
                });
                Lang.forEach(spec.model.getOptions(), s -> {
                    getConst(v, s);
                    v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                            TYPE_BUILDER.getInternalName(), "withOption", //$NON-NLS-1$
                            Type.getMethodDescriptor(TYPE_BUILDER, typeOf(String.class)),
                            false);
                });
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        TYPE_BUILDER.getInternalName(), "build", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(Function.class)),
                        false);
                v.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                        target.getInternalName(), "input", //$NON-NLS-1$
                        Type.getMethodDescriptor(typeOf(JdbcInputAdapter.class),
                                typeOf(String.class), typeOf(String.class), typeOf(Function.class)),
                        false);
                v.visitInsn(Opcodes.POP);
            }
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    private static ClassDescription getAdapter(ClassGeneratorContext context, Spec spec) {
        return SupplierGenerator.get(context, ResultSetAdapterGenerator.get(
                context,
                new ResultSetAdapterGenerator.Spec(
                        spec.model.getDataType(),
                        Lang.project(spec.model.getColumnMappings(), Tuple::right))));
    }

    /**
     * Represents an operation spec for {@link JdbcInputAdapter}.
     * @since 0.4.0
     */
    public static class Spec {

        final String id;

        final WindGateJdbcInputModel model;

        /**
         * Creates a new instance.
         * @param id the input ID
         * @param model the input model
         */
        public Spec(String id, WindGateJdbcInputModel model) {
            Arguments.requireNonNull(id);
            Arguments.requireNonNull(model);
            this.id = id;
            this.model = model;
        }
    }
}
