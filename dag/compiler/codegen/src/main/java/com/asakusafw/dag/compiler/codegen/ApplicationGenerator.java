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
package com.asakusafw.dag.compiler.codegen;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.api.model.GraphInfo;
import com.asakusafw.dag.api.processor.basic.AbstractApplication;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Generates an Asakusa DAG application which inherits {@link AbstractApplication}.
 * @since 0.4.0
 */
public class ApplicationGenerator {

    /**
     * Generates a class.
     * @param contents the {@link GraphInfo} contents
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(Location contents, ClassDescription target) {
        ClassWriter writer = newWriter(target, AbstractApplication.class);
        defineEmptyConstructor(writer, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getConst(method, contents.toPath());
            method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                    typeOf(AbstractApplication.class).getInternalName(),
                    CONSTRUCTOR_NAME,
                    Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(String.class)),
                    false);
        });
        writer.visitEnd();
        return new ClassData(target, writer::toByteArray);
    }
}
