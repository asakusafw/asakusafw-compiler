/**
 * Copyright 2011-2019 Asakusa Framework Team.
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

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.objectweb.asm.MethodVisitor;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorProperty;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.vocabulary.operator.MasterBranch;

/**
 * Generates {@link MasterBranch} operator.
 * @since 0.4.0
 */
public class MasterBranchOperatorGenerator extends MasterJoinLikeOperatorGenerator {

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return MasterBranch.class;
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
        List<ValueRef> arguments = new ArrayList<>();
        arguments.add(impl);
        arguments.add(master);
        arguments.add(transaction);
        appendExtraViews(arguments::add, operator, dependencies::get);
        appendArguments(arguments::add, operator, dependencies::get);
        invoke(method, context, operator, arguments);
        BranchOperatorGenerator.branch(
                method, context, operator,
                transaction,
                dependencies);
    }
}
