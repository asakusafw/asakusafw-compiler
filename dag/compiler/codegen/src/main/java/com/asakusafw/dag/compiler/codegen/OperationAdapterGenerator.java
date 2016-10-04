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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.OperationSpec;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.skeleton.BasicOperationAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Generates {@link OperationAdapter} classes.
 * @since 0.4.0
 */
public class OperationAdapterGenerator {

    private static final String SUFFIX_INNER = "$Op"; //$NON-NLS-1$

    /**
     * Generates {@link OperationAdapter} class.
     * @param context the current context
     * @param operation the target operation
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(ClassGeneratorContext context, OperationSpec operation, ClassDescription target) {
        ClassDescription inner = addInner(context, operation, target);
        ClassWriter writer = newWriter(target, BasicOperationAdapter.class);
        defineAdapterConstructor(writer, BasicOperationAdapter.class, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getConst(v, inner);
            v.visitMethodInsn(
                    Opcodes.INVOKEVIRTUAL,
                    target.getInternalName(),
                    "bind",
                    Type.getMethodDescriptor(typeOf(BasicOperationAdapter.class), typeOf(Class.class)),
                    false);
            v.visitInsn(Opcodes.POP);
        });
        return new ClassData(target, writer::toByteArray);
    }

    private static ClassDescription addInner(
            ClassGeneratorContext context, OperationSpec operation, ClassDescription outer) {
        ClassDescription target = new ClassDescription(outer.getBinaryName() + SUFFIX_INNER);
        OperationGenerator gen = new OperationGenerator();
        ClassData data = gen.generate(context, operation, target);
        context.addClassFile(data);
        return target;
    }
}
