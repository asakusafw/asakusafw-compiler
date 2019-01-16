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
package com.asakusafw.lang.compiler.extension.directio.emitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.CompiledSegment;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.RandomNumber;
import com.asakusafw.lang.compiler.extension.directio.OutputPattern.SourceKind;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.mapreduce.JavaDomUtil;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.runtime.stage.directio.StringTemplate;
import com.asakusafw.runtime.stage.directio.StringTemplate.Format;
import com.asakusafw.runtime.stage.directio.StringTemplate.FormatSpec;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.utils.java.model.syntax.CompilationUnit;
import com.asakusafw.utils.java.model.syntax.ConstructorDeclaration;
import com.asakusafw.utils.java.model.syntax.Expression;
import com.asakusafw.utils.java.model.syntax.FieldDeclaration;
import com.asakusafw.utils.java.model.syntax.InfixOperator;
import com.asakusafw.utils.java.model.syntax.MethodDeclaration;
import com.asakusafw.utils.java.model.syntax.ModelFactory;
import com.asakusafw.utils.java.model.syntax.SimpleName;
import com.asakusafw.utils.java.model.syntax.Statement;
import com.asakusafw.utils.java.model.syntax.Type;
import com.asakusafw.utils.java.model.syntax.TypeBodyDeclaration;
import com.asakusafw.utils.java.model.syntax.TypeDeclaration;
import com.asakusafw.utils.java.model.util.AttributeBuilder;
import com.asakusafw.utils.java.model.util.ExpressionBuilder;
import com.asakusafw.utils.java.model.util.ImportBuilder;
import com.asakusafw.utils.java.model.util.JavadocBuilder;
import com.asakusafw.utils.java.model.util.Models;
import com.asakusafw.utils.java.model.util.TypeBuilder;

/**
 * Emits {@link StringTemplate} subclasses.
 */
public final class StringTemplateClassEmitter {

    static final Logger LOG = LoggerFactory.getLogger(StringTemplateClassEmitter.class);

    private static final String FIELD_RANDOM_HOLDER = "randomValue"; //$NON-NLS-1$

    private static final String FIELD_RANDOMIZER = "randomizer"; //$NON-NLS-1$

    private final ModelFactory f = Models.getModelFactory();

    private final ClassDescription targetClass;

    private final DataModelReference dataType;

    private final List<CompiledSegment> segments;

    private final ImportBuilder importer;

    private StringTemplateClassEmitter(ClassDescription targetClass, OutputStageInfo.Operation operation) {
        assert targetClass != null;
        assert operation != null;
        this.targetClass = targetClass;
        this.dataType = operation.dataModel;
        this.segments = operation.resourcePattern.getResourcePattern();
        this.importer = JavaDomUtil.newImportBuilder(targetClass);
    }

    /**
     * Emits a class.
     * @param targetClass the target class
     * @param output the output information
     * @param javac the Java compiler
     * @throws IOException if failed to emit the class
     */
    public static void emit(
            ClassDescription targetClass,
            OutputStageInfo.Operation output, JavaSourceExtension javac) throws IOException {
        StringTemplateClassEmitter self = new StringTemplateClassEmitter(targetClass, output);
        CompilationUnit source = self.generate();
        JavaDomUtil.emit(source, javac);
    }

    private CompilationUnit generate() {
        TypeDeclaration type = createType();
        return f.newCompilationUnit(
                importer.getPackageDeclaration(),
                importer.toImportDeclarations(),
                Collections.singletonList(type),
                Collections.emptyList());
    }

    private TypeDeclaration createType() {
        SimpleName name = f.newSimpleName(targetClass.getSimpleName());
        importer.resolvePackageMember(name);
        List<TypeBodyDeclaration> members = new ArrayList<>();
        if (requireRandomNumber()) {
            members.add(createRandomHolder());
            members.add(createRandomizer());
        }
        members.add(createConstructor());
        members.add(createSetMethod());
        return f.newClassDeclaration(
                null,
                new AttributeBuilder(f)
                    .annotation(
                            importer.toType(SuppressWarnings.class),
                            Models.toLiteral(f, "deprecation")) //$NON-NLS-1$
                    .Public()
                    .Final()
                    .toAttributes(),
                name,
                Collections.emptyList(),
                t(StringTemplate.class),
                Collections.emptyList(),
                members);
    }

    private boolean requireRandomNumber() {
        for (CompiledSegment naming : segments) {
            if (naming.getKind() == SourceKind.RANDOM) {
                return true;
            }
        }
        return false;
    }

    private FieldDeclaration createRandomHolder() {
        return f.newFieldDeclaration(
                null,
                new AttributeBuilder(f)
                    .Private()
                    .Final()
                    .toAttributes(),
                importer.toType(IntOption.class),
                f.newSimpleName(FIELD_RANDOM_HOLDER),
                new TypeBuilder(f, importer.toType(IntOption.class))
                    .newObject()
                    .toExpression());
    }

    private FieldDeclaration createRandomizer() {
        return f.newFieldDeclaration(
                null,
                new AttributeBuilder(f)
                    .Private()
                    .Final()
                    .toAttributes(),
                importer.toType(Random.class),
                f.newSimpleName(FIELD_RANDOMIZER),
                new TypeBuilder(f, importer.toType(Random.class))
                    .newObject(Models.toLiteral(f, 12345))
                    .toExpression());
    }

    private ConstructorDeclaration createConstructor() {
        List<Expression> arguments = new ArrayList<>();
        for (CompiledSegment naming : segments) {
            arguments.add(new TypeBuilder(f, t(FormatSpec.class))
                .newObject(
                        new TypeBuilder(f, t(Format.class))
                            .field(naming.getFormat().name())
                            .toExpression(),
                        naming.getArgument() == null
                            ? Models.toNullLiteral(f)
                            : Models.toLiteral(f, naming.getArgument()))
                .toExpression());
        }
        List<Statement> statements = new ArrayList<>();
        statements.add(f.newSuperConstructorInvocation(arguments));
        return f.newConstructorDeclaration(
                new JavadocBuilder(f)
                    .text("Creates a new instance.") //$NON-NLS-1$
                    .toJavadoc(),
                new AttributeBuilder(f)
                    .Public()
                    .toAttributes(),
                f.newSimpleName(targetClass.getSimpleName()),
                Collections.emptyList(),
                statements);
    }

    private MethodDeclaration createSetMethod() {
        SimpleName raw = f.newSimpleName("rawObject"); //$NON-NLS-1$
        SimpleName object = f.newSimpleName("object"); //$NON-NLS-1$
        List<Statement> statements = new ArrayList<>();
        statements.add(new ExpressionBuilder(f, raw)
            .castTo(t(dataType.getDeclaration()))
            .toLocalVariableDeclaration(t(dataType.getDeclaration()), object));
        int position = 0;
        for (CompiledSegment naming : segments) {
            switch (naming.getKind()) {
            case NOTHING:
                break;
            case PROPERTY: {
                PropertyReference property = naming.getTarget();
                statements.add(new ExpressionBuilder(f, f.newThis())
                    .method("setProperty", //$NON-NLS-1$
                            Models.toLiteral(f, position),
                            Util.newGetter(object, property))
                    .toStatement());
                break;
            }
            case RANDOM: {
                RandomNumber rand = naming.getRandomNumber();
                statements.add(new ExpressionBuilder(f, f.newThis())
                    .field(FIELD_RANDOM_HOLDER)
                    .method("modify", new ExpressionBuilder(f, f.newThis()) //$NON-NLS-1$
                        .field(FIELD_RANDOMIZER)
                        .method(
                                "nextInt", //$NON-NLS-1$
                                Models.toLiteral(f, rand.getUpperBound() - rand.getLowerBound() + 1))
                        .apply(InfixOperator.PLUS, Models.toLiteral(f, rand.getLowerBound()))
                        .toExpression())
                    .toStatement());
                statements.add(new ExpressionBuilder(f, f.newThis())
                    .method("setProperty", //$NON-NLS-1$
                            Models.toLiteral(f, position),
                            new ExpressionBuilder(f, f.newThis())
                                .field(FIELD_RANDOM_HOLDER)
                                .toExpression())
                    .toStatement());
                break;
            }
            default:
                throw new AssertionError(naming.getKind());
            }
            position++;
        }
        return f.newMethodDeclaration(
                null,
                new AttributeBuilder(f)
                    .annotation(t(Override.class))
                    .Public()
                    .toAttributes(),
                t(void.class),
                f.newSimpleName("set"), //$NON-NLS-1$
                Collections.singletonList(f.newFormalParameterDeclaration(t(Object.class), raw)),
                statements);
    }

    private Type t(java.lang.reflect.Type type) {
        return importer.toType(type);
    }

    private Type t(ClassDescription aClass) {
        return importer.toType(JavaDomUtil.getName(aClass));
    }
}
