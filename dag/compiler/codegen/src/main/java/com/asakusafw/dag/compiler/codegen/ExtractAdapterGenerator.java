/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
import java.util.List;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.ExtractOperation;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.runtime.core.Result;

/**
 * Generates an adapter between {@link ExtractOperation} and {@link Result}.
 * @since 0.4.0
 */
public class ExtractAdapterGenerator {

    private static final ClassDescription RESULT_TYPE = Descriptions.classOf(Result.class);

    /**
     * Generates the class.
     * @param successor the successor
     * @param target the target class
     * @return the generated class
     */
    public ClassData generate(VertexElement successor, ClassDescription target) {
        Arguments.require(successor.getRuntimeType().equals(RESULT_TYPE));
        ClassWriter writer = newWriter(target, Object.class, Result.class);
        List<Tuple<VertexElement, FieldRef>> pairs = defineDependenciesConstructor(
                target, writer, Arrays.asList(successor), Lang.discard());
        Invariants.require(pairs.size() == 1);
        defineResultAdd(writer, method -> {
            pairs.get(0).right().load(method);
            method.visitVarInsn(Opcodes.ALOAD, 1);
            method.visitTypeInsn(Opcodes.CHECKCAST, typeOf(ExtractOperation.Input.class).getInternalName());
            method.visitMethodInsn(Opcodes.INVOKEINTERFACE,
                    typeOf(ExtractOperation.Input.class).getInternalName(),
                    "getObject",
                    Type.getMethodDescriptor(typeOf(Object.class)),
                    true);
            invokeResultAdd(method);
        });
        return new ClassData(target, writer::toByteArray);
    }
}
