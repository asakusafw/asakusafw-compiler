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
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.adapter.DataTableAdapter;
import com.asakusafw.dag.runtime.adapter.InputAdapter;
import com.asakusafw.dag.runtime.adapter.OperationAdapter;
import com.asakusafw.dag.runtime.adapter.OutputAdapter;
import com.asakusafw.dag.runtime.skeleton.VertexAdapter;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Generates {@link VertexAdapter}.
 * @since 0.4.0
 */
public class VertexAdapterGenerator {

    /**
     * Generates {@link VertexAdapter} class.
     * @param context the current context
     * @param inputAdapter the {@link InputAdapter} class
     * @param dataTableAdapters the {@link DataTableAdapter} classes
     * @param operationAdapter the {@link OperationAdapter} class
     * @param outputAdapters the {@link OutputAdapter} classes
     * @param label the vertex label
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(
            ClassGeneratorContext context,
            ClassDescription inputAdapter,
            List<ClassDescription> dataTableAdapters,
            ClassDescription operationAdapter,
            List<ClassDescription> outputAdapters,
            String label,
            ClassDescription target) {
        ClassWriter writer = newWriter(target, VertexAdapter.class);
        defineEmptyConstructor(writer, VertexAdapter.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            putConf(v, target, self, inputAdapter, "input"); //$NON-NLS-1$
            for (ClassDescription dataTableAdapter : dataTableAdapters) {
                putConf(v, target, self, dataTableAdapter, "dataTable"); //$NON-NLS-1$
            }
            putConf(v, target, self, operationAdapter, "operation"); //$NON-NLS-1$
            for (ClassDescription outputAdapter : outputAdapters) {
                putConf(v, target, self, outputAdapter, "output"); //$NON-NLS-1$
            }
        });
        if (label != null) {
            defineToString(writer, label);
        }
        return new ClassData(target, writer::toByteArray);
    }

    private static void putConf(MethodVisitor method, ClassDescription target,
            LocalVarRef self, ClassDescription value, String name) {
        self.load(method);
        getConst(method, value);
        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                target.getInternalName(), name,
                Type.getMethodDescriptor(typeOf(VertexAdapter.class), typeOf(Class.class)),
                false);
        method.visitInsn(Opcodes.POP);
    }
}
