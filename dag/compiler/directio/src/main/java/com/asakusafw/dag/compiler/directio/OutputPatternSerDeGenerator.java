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
package com.asakusafw.dag.compiler.directio;

import static com.asakusafw.dag.compiler.codegen.AsmUtil.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.Label;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import com.asakusafw.dag.compiler.codegen.AsmUtil.FieldRef;
import com.asakusafw.dag.compiler.codegen.AsmUtil.LocalVarRef;
import com.asakusafw.dag.compiler.model.ClassData;
import com.asakusafw.dag.runtime.directio.OutputPatternSerDe;
import com.asakusafw.dag.runtime.io.ValueOptionSerDe;
import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.CompiledOrder;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.CompiledSegment;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;

/**
 * Generates {@link OutputPatternSerDe}.
 * @since 0.4.0
 */
public class OutputPatternSerDeGenerator {

    static final ClassDescription SERDE = Descriptions.classOf(ValueOptionSerDe.class);

    /**
     * Generates {@link OutputPatternSerDe} class.
     * @param reference the target data model reference
     * @param pattern the output pattern
     * @param target the target class
     * @return the generated class data
     */
    public ClassData generate(DataModelReference reference, OutputPattern pattern, ClassDescription target) {
        ClassWriter writer = newWriter(target, OutputPatternSerDe.class);
        FieldRef buffer = defineField(writer, target, "buffer", typeOf(reference));

        putCtor(writer, reference, buffer, pattern, target);
        putGetProperty(writer, reference, pattern);

        List<PropertyReference> properties = getProperties(reference, pattern);
        putSerialize(reference, properties, writer);
        putDeserialize(reference, properties, buffer, writer);

        return new ClassData(target, writer::toByteArray);
    }

    private static List<PropertyReference> getProperties(DataModelReference reference, OutputPattern pattern) {
        List<PropertyReference> results = pattern.getOrders().stream()
                .map(CompiledOrder::getTarget)
                .collect(Collectors.toList());
        Set<PropertyName> saw = results.stream()
                .map(PropertyReference::getName)
                .collect(Collectors.toSet());
        reference.getProperties().stream()
            .filter(p -> saw.contains(p.getName()) == false)
            .forEach(results::add);
        return results;
    }

    private static void putGetProperty(ClassWriter writer, DataModelReference reference, OutputPattern pattern) {
        List<PropertyReference> properties = pattern.getResourcePattern().stream()
                .filter(s -> s.getKind() == OutputPattern.SourceKind.PROPERTY)
                .map(CompiledSegment::getTarget)
                .collect(Collectors.toList());
        if (properties.isEmpty()) {
            return;
        }
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "getProperty",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(Object.class), typeOf(int.class)),
                null,
                null);
        LocalVarRef object = cast(v, 1, reference.getDeclaration());
        LocalVarRef index = new LocalVarRef(Opcodes.ILOAD, 2);
        Label[] caseLabels = properties.stream().map(o -> new Label()).toArray(Label[]::new);
        Label defaultLabel = new Label();

        index.load(v);
        v.visitTableSwitchInsn(
                0, caseLabels.length - 1,
                defaultLabel, caseLabels);

        for (int i = 0, n = properties.size(); i < n; i++) {
            v.visitLabel(caseLabels[i]);
            PropertyReference property = properties.get(i);
            object.load(v);
            getOption(v, property);
            v.visitInsn(Opcodes.ARETURN);
        }
        v.visitLabel(defaultLabel);
        getNew(v, Descriptions.typeOf(AssertionError.class));
        v.visitInsn(Opcodes.ATHROW);

        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void putCtor(
            ClassWriter writer, DataModelReference reference,
            FieldRef buffer,
            OutputPattern pattern, ClassDescription target) {
        defineEmptyConstructor(writer, OutputPatternSerDe.class, v -> {
            LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
            self.load(v);
            getNew(v, reference.getDeclaration());
            putField(v, buffer);

            for (CompiledSegment segment : pattern.getResourcePattern()) {
                self.load(v);
                switch (segment.getKind()) {
                case NOTHING:
                    getConst(v, segment.getArgument());
                    v.visitMethodInsn(
                            Opcodes.INVOKEVIRTUAL,
                            target.getInternalName(), "text", //$NON-NLS-1$
                            Type.getMethodDescriptor(typeOf(OutputPatternSerDe.class),
                                    typeOf(String.class)),
                            false);
                    break;
                case PROPERTY:
                    switch (segment.getFormat()) {
                    case NATURAL:
                        getEnumConstant(v, OutputPatternSerDe.Format.NATURAL);
                        break;
                    case DATE:
                        getEnumConstant(v, OutputPatternSerDe.Format.DATE);
                        break;
                    case DATETIME:
                        getEnumConstant(v, OutputPatternSerDe.Format.DATETIME);
                        break;
                    default:
                        throw new AssertionError(pattern.getResourcePatternString());
                    }
                    getConst(v, segment.getArgument());
                    v.visitMethodInsn(
                            Opcodes.INVOKEVIRTUAL,
                            target.getInternalName(), "property", //$NON-NLS-1$
                            Type.getMethodDescriptor(typeOf(OutputPatternSerDe.class),
                                    typeOf(OutputPatternSerDe.Format.class), typeOf(String.class)),
                            false);
                    break;
                case RANDOM:
                    getInt(v, segment.getRandomNumber().getLowerBound());
                    getInt(v, segment.getRandomNumber().getUpperBound());
                    v.visitMethodInsn(
                            Opcodes.INVOKEVIRTUAL,
                            target.getInternalName(), "random", //$NON-NLS-1$
                            Type.getMethodDescriptor(typeOf(OutputPatternSerDe.class),
                                    typeOf(int.class), typeOf(int.class)),
                            false);
                    break;
                default:
                    throw new AssertionError(pattern.getResourcePatternString());
                }
                v.visitInsn(Opcodes.POP);
            }
        });
    }

    private static void putSerialize(
            DataModelReference reference, List<PropertyReference> properties,
            ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "serializeValue",
                Type.getMethodDescriptor(Type.VOID_TYPE, typeOf(Object.class), typeOf(DataOutput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        LocalVarRef object = cast(v, 1, reference.getDeclaration());
        LocalVarRef output = new LocalVarRef(Opcodes.ALOAD, 2);
        for (PropertyReference property : properties) {
            object.load(v);
            getOption(v, property);
            output.load(v);
            v.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    SERDE.getInternalName(),
                    "serialize",
                    Type.getMethodDescriptor(
                            Type.VOID_TYPE,
                            typeOf(property.getType()),
                            typeOf(DataOutput.class)),
                    false);
        }
        v.visitInsn(Opcodes.RETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }

    private static void putDeserialize(
            DataModelReference reference, List<PropertyReference> properties,
            FieldRef buffer, ClassWriter writer) {
        MethodVisitor v = writer.visitMethod(
                Opcodes.ACC_PUBLIC,
                "deserializePair",
                Type.getMethodDescriptor(typeOf(Object.class), typeOf(DataInput.class), typeOf(DataInput.class)),
                null,
                new String[] {
                        typeOf(IOException.class).getInternalName(),
                        typeOf(InterruptedException.class).getInternalName(),
                });
        LocalVarRef self = new LocalVarRef(Opcodes.ALOAD, 0);
        LocalVarRef valueInput = new LocalVarRef(Opcodes.ALOAD, 2);
        self.load(v);
        getField(v, buffer);
        LocalVarRef object = putLocalVar(v, Type.OBJECT, 3);
        for (PropertyReference property : properties) {
            object.load(v);
            getOption(v, property);
            valueInput.load(v);
            v.visitMethodInsn(
                    Opcodes.INVOKESTATIC,
                    SERDE.getInternalName(),
                    "deserialize",
                    Type.getMethodDescriptor(
                            Type.VOID_TYPE,
                            typeOf(property.getType()),
                            typeOf(DataInput.class)),
                    false);
        }
        object.load(v);
        v.visitInsn(Opcodes.ARETURN);
        v.visitMaxs(0, 0);
        v.visitEnd();
    }
}
