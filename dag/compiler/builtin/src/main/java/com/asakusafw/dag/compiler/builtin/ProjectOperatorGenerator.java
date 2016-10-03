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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Opcodes;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.ValueRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.lang.compiler.analyzer.util.OperatorUtil;
import com.asakusafw.lang.compiler.analyzer.util.ProjectionOperatorUtil;
import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping;
import com.asakusafw.lang.compiler.api.DataModelLoader;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.graph.CoreOperator;
import com.asakusafw.lang.compiler.model.graph.CoreOperator.CoreOperatorKind;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.vocabulary.operator.Project;

/**
 * Generates {@link Project} operator.
 * @since 0.4.0
 */
public class ProjectOperatorGenerator extends CoreOperatorNodeGenerator {

    @Override
    protected CoreOperatorKind getOperatorKind() {
        return CoreOperatorKind.PROJECT;
    }

    @Override
    protected NodeInfo generate(Context context, CoreOperator operator, Supplier<? extends ClassDescription> namer) {
        return ProjectOperatorGenerator.gen(context, operator, namer);
    }

    static OperatorNodeInfo gen(Context context, CoreOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i == 1, i -> i == 1);
        checkArgs(operator, i -> i == 0);
        OperatorUtil.checkOperatorPorts(operator, 1, 1);

        return new OperatorNodeInfo(
                context.cache(CacheKey.of(operator), () -> genClass(context, operator, namer.get())),
                operator.getInputs().get(Project.ID_INPUT).getDataType(),
                getDependencies(context, operator));
    }

    private static List<VertexElement> getDependencies(Context context, CoreOperator operator) {
        return context.getDependencies(operator.getOutputs());
    }

    private static ClassData genClass(Context context, CoreOperator operator, ClassDescription target) {
        DataModelLoader loader = context.getDataModelLoader();
        DataModelReference inputType = loader.load(operator.getInputs().get(Project.ID_INPUT).getDataType());
        DataModelReference outputType = loader.load(operator.getOutputs().get(Project.ID_OUTPUT).getDataType());
        List<PropertyMapping> mappings = ProjectionOperatorUtil.getPropertyMappings(loader, operator);

        ClassWriter writer = newWriter(target, Object.class, Result.class);
        FieldRef bufferField = defineField(writer, target, "buffer", typeOf(outputType));

        List<VertexElement> dependencies = getDependencies(context, operator);
        Map<VertexElement, FieldRef> deps = defineDependenciesConstructor(target, writer, dependencies, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, outputType.getDeclaration());
            putField(method, bufferField);
        });
        defineResultAdd(writer, method -> {
            cast(method, 1, inputType.getDeclaration());

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, bufferField);
            method.visitVarInsn(Opcodes.ASTORE, 2);

            Map<OperatorInput, ValueRef> inputs = new HashMap<>();
            Map<OperatorOutput, ValueRef> outputs = new HashMap<>();
            inputs.put(operator.getInputs().get(Project.ID_INPUT), new LocalVarRef(Opcodes.ALOAD, 1));
            outputs.put(operator.getOutputs().get(Project.ID_OUTPUT), new LocalVarRef(Opcodes.ALOAD, 2));
            mapping(method, loader, mappings, inputs, outputs);

            method.visitVarInsn(Opcodes.ALOAD, 0);
            getField(method, deps.get(dependencies.get(0)));
            method.visitVarInsn(Opcodes.ALOAD, 2);
            invokeResultAdd(method);
        });
        return new ClassData(target, writer::toByteArray);
    }
}
