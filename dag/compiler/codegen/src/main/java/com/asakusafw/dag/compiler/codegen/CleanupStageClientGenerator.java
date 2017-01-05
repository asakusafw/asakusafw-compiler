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

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.stage.AbstractCleanupStageClient;
import com.asakusafw.runtime.stage.BaseStageClient;

/**
 * Generates a subclass of {@link AbstractCleanupStageClient}.
 * @since 0.4.0
 */
public class CleanupStageClientGenerator {

    /**
     * The default cleanup class.
     */
    public static final ClassDescription DEFAULT_CLASS =
            new ClassDescription(AbstractCleanupStageClient.IMPLEMENTATION);

    /**
     * The default cleanup stage ID.
     */
    public static final String DEFAULT_STAGE_ID = "cleanup"; //$NON-NLS-1$

    /**
     * Generates a subclass of {@link AbstractCleanupStageClient}.
     * @param batchId the current batch ID
     * @param flowId the current flow ID
     * @param path the cleanup target path
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(String batchId, String flowId, String path, ClassDescription target) {
        ClassWriter writer = newWriter(target, AbstractCleanupStageClient.class);
        defineEmptyConstructor(writer, AbstractCleanupStageClient.class);
        defineString(writer, BaseStageClient.METHOD_BATCH_ID, batchId);
        defineString(writer, BaseStageClient.METHOD_FLOW_ID, flowId);
        defineString(writer, BaseStageClient.METHOD_STAGE_ID, DEFAULT_STAGE_ID);
        defineString(writer, AbstractCleanupStageClient.METHOD_CLEANUP_PATH, path);
        return new ClassData(target, writer::toByteArray);
    }

    private static void defineString(ClassWriter writer, String name, String value) {
        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PROTECTED,
                name,
                Type.getMethodDescriptor(typeOf(String.class)),
                null,
                new String[0]);
        getConst(method, value);
        method.visitInsn(Opcodes.ARETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
    }
}
