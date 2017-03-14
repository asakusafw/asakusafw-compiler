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
package com.asakusafw.dag.compiler.builtin;

import static com.asakusafw.dag.compiler.builtin.Util.*;
import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.lang.annotation.Annotation;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

import org.apache.hadoop.io.Text;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.codegen.ObjectCopierGenerator;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.compiler.model.graph.VertexElement;
import com.asakusafw.dag.runtime.adapter.ObjectCombiner;
import com.asakusafw.dag.runtime.skeleton.CombineResult;
import com.asakusafw.lang.compiler.analyzer.util.PropertyFolding;
import com.asakusafw.lang.compiler.analyzer.util.PropertyFolding.Aggregation;
import com.asakusafw.lang.compiler.analyzer.util.PropertyMapping;
import com.asakusafw.lang.compiler.analyzer.util.SummarizedModelUtil;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.graph.OperatorInput;
import com.asakusafw.lang.compiler.model.graph.OperatorOutput;
import com.asakusafw.lang.compiler.model.graph.UserOperator;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Lang;
import com.asakusafw.runtime.core.Result;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.value.BooleanOption;
import com.asakusafw.runtime.value.ByteOption;
import com.asakusafw.runtime.value.Date;
import com.asakusafw.runtime.value.DateOption;
import com.asakusafw.runtime.value.DateTime;
import com.asakusafw.runtime.value.DateTimeOption;
import com.asakusafw.runtime.value.DecimalOption;
import com.asakusafw.runtime.value.DoubleOption;
import com.asakusafw.runtime.value.FloatOption;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.LongOption;
import com.asakusafw.runtime.value.ShortOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;
import com.asakusafw.vocabulary.operator.Summarize;

/**
 * Generates {@link Summarize} operator.
 * @since 0.4.0
 */
public class SummarizeOperatorGenerator extends UserOperatorNodeGenerator {

    private static final TypeDescription VALUE_DESC = Descriptions.typeOf(ValueOption.class);
    private static final TypeDescription BYTE_DESC = Descriptions.typeOf(ByteOption.class);
    private static final TypeDescription SHORT_DESC = Descriptions.typeOf(ShortOption.class);
    private static final TypeDescription INT_DESC = Descriptions.typeOf(IntOption.class);
    private static final TypeDescription LONG_DESC = Descriptions.typeOf(LongOption.class);
    private static final TypeDescription FLOAT_DESC = Descriptions.typeOf(FloatOption.class);

    private static final Map<TypeDescription, Type> ENTITY_TYPE_MAP = Lang.let(new HashMap<>(), m -> {
        m.put(Descriptions.typeOf(BooleanOption.class), Type.BOOLEAN_TYPE);
        m.put(Descriptions.typeOf(ByteOption.class), Type.BYTE_TYPE);
        m.put(Descriptions.typeOf(ShortOption.class), Type.SHORT_TYPE);
        m.put(Descriptions.typeOf(IntOption.class), Type.INT_TYPE);
        m.put(Descriptions.typeOf(LongOption.class), Type.LONG_TYPE);
        m.put(Descriptions.typeOf(FloatOption.class), Type.FLOAT_TYPE);
        m.put(Descriptions.typeOf(DoubleOption.class), Type.DOUBLE_TYPE);
        m.put(Descriptions.typeOf(DecimalOption.class), typeOf(BigDecimal.class));
        m.put(Descriptions.typeOf(DateOption.class), typeOf(Date.class));
        m.put(Descriptions.typeOf(DateTimeOption.class), typeOf(DateTime.class));
        m.put(Descriptions.typeOf(StringOption.class), typeOf(Text.class));
    });

    static final String SUFFIX_MAPPER = "$Mapper";

    static final String SUFFIX_COMBINER = "$Combiner";

    @Override
    protected Class<? extends Annotation> getAnnotationClass() {
        return Summarize.class;
    }

    @Override
    protected NodeInfo generate(Context context, UserOperator operator, Supplier<? extends ClassDescription> namer) {
        checkPorts(operator, i -> i == 1, i -> i == 1);
        checkArgs(operator, i -> i == 0);
        CacheKey key = CacheKey.builder()
                .operator(operator)
                .arguments(operator) // aggregate operation embeds its arguments into combiner class (always empty)
                .build();
        ClassData adapter = context.cache(key, () -> generateClass(context, operator, namer.get()));
        return new AggregateNodeInfo(
                adapter,
                getMapperName(adapter.getDescription()),
                ObjectCopierGenerator.get(context, operator.getOutput(Summarize.ID_OUTPUT).getDataType()),
                getCombinerName(adapter.getDescription()),
                operator.getInput(Summarize.ID_INPUT).getDataType(),
                operator.getOutput(Summarize.ID_OUTPUT).getDataType(),
                getDependencies(context, operator));
    }

    private static ClassDescription getMapperName(ClassDescription outer) {
        return new ClassDescription(outer.getBinaryName() + SUFFIX_MAPPER);
    }

    private static ClassDescription getCombinerName(ClassDescription outer) {
        return new ClassDescription(outer.getBinaryName() + SUFFIX_COMBINER);
    }

    private static List<VertexElement> getDependencies(Context context, UserOperator operator) {
        return getDefaultDependencies(context, operator);
    }

    private static ClassData generateClass(Context context, UserOperator operator, ClassDescription target) {
        ClassDescription mapperClass = generateMapperClass(context, operator, target);
        ClassDescription combinerClass = generateCombinerClass(context, operator, target);

        ClassWriter writer = newWriter(target, CombineResult.class);
        writer.visitInnerClass(
                mapperClass.getInternalName(),
                target.getInternalName(),
                mapperClass.getSimpleName(),
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC);
        writer.visitInnerClass(
                combinerClass.getInternalName(),
                target.getInternalName(),
                combinerClass.getSimpleName(),
                Opcodes.ACC_PUBLIC | Opcodes.ACC_STATIC);

        OperatorOutput output = operator.getOutput(Summarize.ID_OUTPUT);
        defineDependenciesConstructor(context, operator.getOutputs(), target, writer,
                method -> {
                    method.visitVarInsn(Opcodes.ALOAD, 0);
                    getNew(method, combinerClass);
                    getNew(method, output.getDataType());
                    method.visitVarInsn(Opcodes.ALOAD, 1);
                    method.visitMethodInsn(Opcodes.INVOKESPECIAL,
                            typeOf(CombineResult.class).getInternalName(),
                            "<init>",
                            Type.getMethodDescriptor(Type.VOID_TYPE,
                                    typeOf(ObjectCombiner.class), typeOf(DataModel.class), typeOf(Result.class)),
                            false);
                },
                Lang.discard());

        return new ClassData(target, writer::toByteArray);
    }

    static ClassDescription generateMapperClass(Context context, UserOperator operator, ClassDescription outer) {
        ClassDescription target = getMapperName(outer);

        OperatorInput input = operator.getInput(Summarize.ID_INPUT);
        OperatorOutput output = operator.getOutput(Summarize.ID_OUTPUT);

        ClassWriter writer = newWriter(target, Object.class, Function.class);
        writer.visitOuterClass(outer.getInternalName(), target.getSimpleName(), null);

        FieldRef buffer = defineField(writer, target, "buffer", typeOf(output.getDataType()));
        defineEmptyConstructor(writer, Object.class, method -> {
            method.visitVarInsn(Opcodes.ALOAD, 0);
            getNew(method, output.getDataType());
            putField(method, buffer);
        });

        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "apply",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(Object.class)),
                null,
                null);

        LocalVarRef inputVar = cast(method, 1, input.getDataType());
        buffer.load(method);
        LocalVarRef outputVar = putLocalVar(method, Type.OBJECT, 2);

        outputVar.load(method);
        method.visitMethodInsn(
                Opcodes.INVOKEVIRTUAL,
                typeOf(output.getDataType()).getInternalName(),
                "reset",
                Type.getMethodDescriptor(Type.VOID_TYPE),
                false);

        List<PropertyFolding> foldings = Invariants.safe(
                () -> SummarizedModelUtil.getPropertyFoldings(context.getClassLoader(), operator));
        DataModelReference inputModel = context.getDataModelLoader().load(input.getDataType());
        DataModelReference outputModel = context.getDataModelLoader().load(output.getDataType());
        for (PropertyFolding folding : foldings) {
            PropertyMapping mapping = folding.getMapping();
            Aggregation aggregation = folding.getAggregation();
            PropertyReference src = Invariants.requireNonNull(
                    inputModel.findProperty(mapping.getSourceProperty()));
            PropertyReference dst = Invariants.requireNonNull(
                    outputModel.findProperty(mapping.getDestinationProperty()));
            mapping(method, aggregation, src, dst, inputVar, outputVar);
        }

        outputVar.load(method);
        method.visitInsn(Opcodes.ARETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
        return context.addClassFile(new ClassData(target, writer::toByteArray));
    }

    private static void mapping(
            MethodVisitor method,
            Aggregation aggregation,
            PropertyReference src, PropertyReference dst,
            LocalVarRef srcVar, LocalVarRef dstVar) {
        switch (aggregation) {
        case COUNT:
            countMapper(method, src, dst, srcVar, dstVar);
            break;
        case ANY:
            simpleMapper(method, src, dst, srcVar, dstVar);
            break;
        case MAX:
            simpleMapper(method, src, dst, srcVar, dstVar);
            break;
        case MIN:
            simpleMapper(method, src, dst, srcVar, dstVar);
            break;
        case SUM:
            sumMapper(method, src, dst, srcVar, dstVar);
            break;
        default:
            throw new AssertionError(aggregation);
        }
    }

    private static void countMapper(
            MethodVisitor method,
            PropertyReference src, PropertyReference dst,
            LocalVarRef srcVar, LocalVarRef dstVar) {
        Invariants.require(dst.getType().equals(LONG_DESC));

        dstVar.load(method);
        getOption(method, dst);
        getConst(method, 1L);
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(LONG_DESC).getInternalName(),
                "modify",
                Type.getMethodDescriptor(typeOf(LONG_DESC), Type.LONG_TYPE),
                false);
    }

    private static void simpleMapper(
            MethodVisitor method,
            PropertyReference src, PropertyReference dst,
            LocalVarRef srcVar, LocalVarRef dstVar) {
        Invariants.require(src.getType().equals(dst.getType()));
        dstVar.load(method);
        getOption(method, dst);
        srcVar.load(method);
        getOption(method, src);
        copyOption(method, src.getType());
    }

    private static void sumMapper(
            MethodVisitor method,
            PropertyReference src, PropertyReference dst,
            LocalVarRef srcVar, LocalVarRef dstVar) {
        dstVar.load(method);
        getOption(method, dst);

        srcVar.load(method);
        getOption(method, src);

        TypeDescription srcOptionType = src.getType();
        Type srcEntityType = Invariants.requireNonNull(ENTITY_TYPE_MAP.get(srcOptionType));
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(srcOptionType).getInternalName(),
                "get",
                Type.getMethodDescriptor(srcEntityType),
                false);
        if (srcOptionType.equals(BYTE_DESC) || srcOptionType.equals(SHORT_DESC) || srcOptionType.equals(INT_DESC)) {
            method.visitInsn(Opcodes.I2L);
        } else if (srcOptionType.equals(FLOAT_DESC)) {
            method.visitInsn(Opcodes.F2D);
        }
        TypeDescription dstOptionType = dst.getType();
        Type dstEntityType = Invariants.requireNonNull(ENTITY_TYPE_MAP.get(dstOptionType));
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                typeOf(dstOptionType).getInternalName(),
                "modify",
                Type.getMethodDescriptor(typeOf(dstOptionType), dstEntityType),
                false);
    }

    static ClassDescription generateCombinerClass(Context context, UserOperator operator, ClassDescription outer) {
        ClassDescription target = getCombinerName(outer);
        OperatorInput input = operator.getInput(Summarize.ID_INPUT);
        OperatorOutput output = operator.getOutput(Summarize.ID_OUTPUT);

        ClassWriter writer = newWriter(target, Object.class, ObjectCombiner.class);
        writer.visitOuterClass(outer.getInternalName(), target.getSimpleName(), null);
        defineEmptyConstructor(writer, Object.class);
        defineBuildKey(context, writer, output.getDataType(), input.getGroup());

        MethodVisitor method = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "combine",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class), typeOf(Object.class)),
                null,
                null);

        LocalVarRef leftVar = cast(method, 1, output.getDataType());
        LocalVarRef rightVar = cast(method, 2, output.getDataType());

        List<PropertyFolding> foldings = Invariants.safe(
                () -> SummarizedModelUtil.getPropertyFoldings(context.getClassLoader(), operator));
        DataModelReference outputModel = context.getDataModelLoader().load(output.getDataType());
        for (PropertyFolding folding : foldings) {
            PropertyMapping mapping = folding.getMapping();
            Aggregation aggregation = folding.getAggregation();
            PropertyReference property = Invariants.requireNonNull(
                    outputModel.findProperty(mapping.getDestinationProperty()));
            combine(method, aggregation, property, leftVar, rightVar);
        }

        method.visitInsn(Opcodes.RETURN);
        method.visitMaxs(0, 0);
        method.visitEnd();
        return context.addClassFile(new ClassData(target, writer::toByteArray));
    }

    private static void combine(
            MethodVisitor method,
            Aggregation aggregation,
            PropertyReference prop,
            LocalVarRef leftVar, LocalVarRef rightVar) {
        switch (aggregation) {
        case ANY:
            // do nothing
            break;
        case MAX:
            combine(method, "max", prop, leftVar, rightVar, true);
            break;
        case MIN:
            combine(method, "min", prop, leftVar, rightVar, true);
            break;
        case SUM:
        case COUNT:
            combine(method, "add", prop, leftVar, rightVar, false);
            break;
        default:
            throw new AssertionError(aggregation);
        }
    }

    private static void combine(
            MethodVisitor method,
            String targetName,
            PropertyReference prop,
            LocalVarRef leftVar, LocalVarRef rightVar,
            boolean parent) {
        leftVar.load(method);
        getOption(method, prop);
        rightVar.load(method);
        getOption(method, prop);
        Type declaring = typeOf(parent ? VALUE_DESC : prop.getType());
        method.visitMethodInsn(Opcodes.INVOKEVIRTUAL,
                declaring.getInternalName(),
                targetName,
                Type.getMethodDescriptor(Type.VOID_TYPE, declaring),
                false);
    }
}
