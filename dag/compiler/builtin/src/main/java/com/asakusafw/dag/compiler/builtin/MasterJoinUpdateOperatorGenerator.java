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
package com.asakusafw.dag.compiler.builtin;

import static com.asakusafw.dag.compiler.builtin.Util.*;
import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.MasterCheck;
import com.asakusafw.vocabulary.operator.MasterJoinUpdate;

/**
 * Generates {@link MasterJoinUpdate} operator.
 * @since 0.4.0
 */
public class MasterJoinUpdateOperatorGenerator extends MasterJoinLikeOperatorGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return MasterJoinUpdate.class;
    }

    @Override
    protected void defineProcess(
            MethodVisitor method,
            Context context,
            UserOperator operator,
            LocalVarRef master, LocalVarRef transaction,
            FieldRef impl,
            Map<OperatorProperty, FieldRef> dependencies,
            ClassDescription target) {
        OperatorOutput found = operator.getOutputs().get(MasterCheck.ID_OUTPUT_FOUND);
        OperatorOutput missed = operator.getOutputs().get(MasterCheck.ID_OUTPUT_MISSED);

        Label onNull = new Label();
        Label end = new Label();
        master.load(method);
        getConst(method, null);
        method.visitJumpInsn(Opcodes.IF_ACMPEQ, onNull);

        List<ValueRef> arguments = new ArrayList<>();
        arguments.add(impl);
        arguments.add(master);
        arguments.add(transaction);
        appendExtraDataTables(arguments::add, operator, dependencies::get);
        appendArguments(arguments::add, operator, dependencies::get);
        invoke(method, context, operator, arguments);
        dependencies.get(found).load(method);
        transaction.load(method);
        invokeResultAdd(method);
        method.visitJumpInsn(Opcodes.GOTO, end);

        method.visitLabel(onNull);
        dependencies.get(missed).load(method);
        transaction.load(method);
        invokeResultAdd(method);

        method.visitLabel(end);
    }
}
