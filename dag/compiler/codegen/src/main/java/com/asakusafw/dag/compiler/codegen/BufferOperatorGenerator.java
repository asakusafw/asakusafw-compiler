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

import java.util.List;
import java.util.Objects;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.DataNode;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.ReifiableTypeDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.runtime.core.Result;

/**
 * Generates buffer operator classes.
 * @since 0.4.0
 */
public final class BufferOperatorGenerator {

    private static final ClassDescription RESULT_TYPE = Descriptions.classOf(Result.class);

    private static final String CATEGORY = "buffer"; //$NON-NLS-1$

    private static final String SUFFIX = "Buffer"; //$NON-NLS-1$

    private BufferOperatorGenerator() {
        return;
    }

    /**
     * Generates buffer operator class.
     * @param context the current context
     * @param successors the successors
     * @return the generated class
     */
    public static ClassDescription get(ClassGeneratorContext context, List<? extends VertexElement> successors) {
        return context.addClassFile(generate(context, successors));
    }

    /**
     * Generates buffer operator class.
     * @param context the current context
     * @param successors the successors
     * @return the generated class data
     */
    public static ClassData generate(ClassGeneratorContext context, List<? extends VertexElement> successors) {
        TypeDescription type = getDataType(successors);
        return context.cache(new Key(type, successors.size()), () -> {
            return generate0(successors, context.getClassName(CATEGORY, NameUtil.getSimpleNameHint(type, SUFFIX)));
        });
    }

    private static ClassData generate0(List<? extends VertexElement> successors, ClassDescription target) {
        Arguments.require(successors.size() >= 2);
        TypeDescription dataType = getDataType(successors);
        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef buffer = defineField(writer, target, "buffer", typeOf(dataType));
        List<Tuple<VertexElement, FieldRef>> pairs = defineDependenciesConstructor(target, writer, successors, v -> {
            v.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(v, dataType);
            putField(v, buffer);
        });
        defineResultAdd(writer, method -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            LocalVarRef input = cast(method, 1, dataType);
            self.load(method);
            getField(method, buffer);
            LocalVarRef buf = putLocalVar(method, Type.OBJECT, 2);
            for (int i = 0, n = pairs.size(); i < n; i++) {
                self.load(method);
                getField(method, pairs.get(i).right());
                if (i < n - 1) {
                    buf.load(method);
                    input.load(method);
                    copyDataModel(method, dataType);
                    buf.load(method);
                } else {
                    input.load(method);
                }
                invokeResultAdd(method);
            }
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }

    private static TypeDescription getDataType(List<? extends VertexElement> successors) {
        TypeDescription dataType = null;
        for (VertexElement element : successors) {
            Arguments.require(element instanceof DataNode);
            DataNode sink = (DataNode) element;
            if (dataType == null) {
                dataType = sink.getDataType().getErasure();
            } else {
                Arguments.require(dataType.equals(sink.getDataType()));
            }
            ReifiableTypeDescription erasure = element.getRuntimeType().getErasure();
            Arguments.require(erasure.equals(RESULT_TYPE));
        }
        return dataType;
    }

    private static class Key {

        private final TypeDescription type;

        private final int count;

        Key(TypeDescription type, int count) {
            this.type = type;
            this.count = count;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(type);
            result = prime * result + count;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Key other = (Key) obj;
            if (!Objects.equals(type, other.type)) {
                return false;
            }
            if (count != other.count) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return String.format("Buffer(%s*%,d)", type, count); //$NON-NLS-1$
        }
    }
}
