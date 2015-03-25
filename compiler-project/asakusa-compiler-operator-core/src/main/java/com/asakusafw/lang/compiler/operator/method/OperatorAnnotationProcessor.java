/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.operator.method;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.annotation.processing.Completion;
import javax.annotation.processing.ProcessingEnvironment;
import javax.annotation.processing.Processor;
import javax.annotation.processing.RoundEnvironment;
import javax.lang.model.SourceVersion;
import javax.lang.model.element.AnnotationMirror;
import javax.lang.model.element.Element;
import javax.lang.model.element.ElementKind;
import javax.lang.model.element.ExecutableElement;
import javax.lang.model.element.Modifier;
import javax.lang.model.element.TypeElement;
import javax.lang.model.util.ElementFilter;
import javax.tools.Diagnostic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.operator.CompileEnvironment;
import com.asakusafw.lang.compiler.operator.Constants;
import com.asakusafw.lang.compiler.operator.OperatorDriver;
import com.asakusafw.lang.compiler.operator.model.OperatorClass;

/**
 * Processes Asakusa Operator Annotations.
 */
public class OperatorAnnotationProcessor implements Processor {

    static final Logger LOG = LoggerFactory.getLogger(OperatorAnnotationProcessor.class);

    private volatile CompileEnvironment environment;

    /**
     * Creates a new instance.
     */
    public OperatorAnnotationProcessor() {
        LOG.debug("creating operator annotation processor: {}", this); //$NON-NLS-1$
    }

    @Override
    public void init(ProcessingEnvironment processingEnv) {
        LOG.debug("initializing operator annotation processor: {}", this); //$NON-NLS-1$
        try {
            this.environment = createCompileEnvironment(processingEnv);
        } catch (RuntimeException e) {
            processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    MessageFormat.format(
                            "failed to initialize Asakusa Operator Compiler ({0})",
                            e.toString()));
            LOG.error("failed to initialize Asakusa Operator Compiler", e);
        } catch (LinkageError e) {
            processingEnv.getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    MessageFormat.format(
                            "failed to initialize Asakusa Operator Compiler by linkage error ({0})",
                            e.toString()));
            LOG.error("failed to initialize Asakusa Operator Compiler by linkage error", e);
            throw e;
        }
    }

    /**
     * Creates a compile environment for this processing (for testing).
     * @param processingEnv current processing environment
     * @return created environment
     */
    protected CompileEnvironment createCompileEnvironment(ProcessingEnvironment processingEnv) {
        return CompileEnvironment.newInstance(
                processingEnv,
                CompileEnvironment.Support.DATA_MODEL_REPOSITORY,
                CompileEnvironment.Support.OPERATOR_DRIVER,
                CompileEnvironment.Support.STRICT_CHECKING,
                CompileEnvironment.Support.FORCE_GENERATE_IMPLEMENTATION);
    }

    @Override
    public Set<String> getSupportedOptions() {
        return Collections.emptySet();
    }

    @Override
    public SourceVersion getSupportedSourceVersion() {
        return Constants.getSupportedSourceVersion();
    }

    @Override
    public Set<String> getSupportedAnnotationTypes() {
        if (environment == null) {
            return Collections.singleton("*");
        }
        Set<String> results = new HashSet<>();
        for (OperatorDriver driver : environment.getOperatorDrivers()) {
            ClassDescription annotationType = driver.getAnnotationTypeName();
            results.add(annotationType.getClassName());
        }
        return results;
    }

    @Override
    public Iterable<? extends Completion> getCompletions(
            Element element,
            AnnotationMirror annotation,
            ExecutableElement member,
            String userText) {
        if (environment == null) {
            return Collections.emptySet();
        }
        if (element.getKind() != ElementKind.METHOD) {
            return Collections.emptyList();
        }
        OperatorDriver driver = this.environment.findDriver((TypeElement) annotation.getAnnotationType().asElement());
        if (driver == null) {
            return Collections.emptyList();
        } else {
            ExecutableElement method = (ExecutableElement) element;
            OperatorDriver.Context context = new OperatorDriver.Context(environment, annotation, method);
            return driver.getCompletions(context, member, userText);
        }
    }

    @Override
    public boolean process(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        if (environment == null) {
            return false;
        }
        LOG.debug("starting operator annotation processor: {}", this); //$NON-NLS-1$
        try {
            if (annotations.isEmpty() == false) {
                run(annotations, roundEnv);
            }
        } catch (RuntimeException e) {
            environment.getProcessingEnvironment().getMessager().printMessage(
                    Diagnostic.Kind.ERROR,
                    MessageFormat.format(
                            "failed to compile Asakusa Operators ({0})",
                            e.toString()));
            LOG.error("failed to compile Asakusa Operators by unknown exception", e);
        }
        return false;
    }

    private void run(Set<? extends TypeElement> annotations, RoundEnvironment roundEnv) {
        assert annotations != null;
        assert roundEnv != null;
        OperatorMethodAnalyzer analyzer = new OperatorMethodAnalyzer(environment);
        for (TypeElement annotation : annotations) {
            Set<ExecutableElement> methods = ElementFilter.methodsIn(roundEnv.getElementsAnnotatedWith(annotation));
            for (ExecutableElement method : methods) {
                analyzer.register(annotation, method);
            }
        }
        Collection<OperatorClass> operatorClasses = analyzer.resolve();
        LOG.debug("found {} operator classes", operatorClasses.size()); //$NON-NLS-1$
        OperatorFactoryEmitter factoryEmitter = new OperatorFactoryEmitter(environment);
        OperatorImplementationEmitter implementationEmitter = new OperatorImplementationEmitter(environment);
        for (OperatorClass aClass : operatorClasses) {
            LOG.debug("emitting support class: {}", aClass.getDeclaration().getQualifiedName()); //$NON-NLS-1$
            factoryEmitter.emit(aClass);
            if (environment.isForceGenerateImplementation()
                    || aClass.getDeclaration().getModifiers().contains(Modifier.ABSTRACT)) {
                implementationEmitter.emit(aClass);
            }
        }
    }
}
