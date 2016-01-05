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
package com.asakusafw.lang.compiler.javac;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.ToolProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * An abstract implementation of {@link JavaCompilerSupport} using JSR-199.
 */
public abstract class AbstractJavaCompilerSupport implements JavaCompilerSupport {

    static final Logger LOG = LoggerFactory.getLogger(AbstractJavaCompilerSupport.class);

    static final String JAVA_EXTENSION = ".java"; //$NON-NLS-1$

    static final Charset DEFAULT_ENCODING = Charset.forName("UTF-8"); //$NON-NLS-1$

    static final String DEFAULT_COMPLIANT_VERSION = "1.7"; //$NON-NLS-1$

    @Override
    public final Writer addJavaFile(ClassDescription aClass) throws IOException {
        Location location = toLocation(aClass);
        OutputStream output = addResource(location);
        return new OutputStreamWriter(output, getEncoding());
    }

    /**
     * Adds a new Java source file and returns its output stream.
     * @param location the source file location
     * @return the output stream to set the target file contents
     * @throws IOException if failed to create a new file
     */
    protected abstract OutputStream addResource(Location location) throws IOException;

    private Location toLocation(ClassDescription aClass) {
        Location location = Location.of(aClass.getInternalName());
        location = new Location(location.getParent(), location.getName() + JAVA_EXTENSION);
        return location;
    }

    @Override
    public final void process() {
        if (isCompileRequired() == false) {
            return;
        }
        JavaCompiler compiler = getJavaCompiler();
        try {
            doCompile(compiler);
        } catch (IOException e) {
            throw new DiagnosticException(
                    com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                    "failed to compile Java sources",
                    e);
        }
    }

    /**
     * Returns the source encoding.
     * @return the source encoding
     */
    public Charset getEncoding() {
        return DEFAULT_ENCODING;
    }

    /**
     * Returns the available Java compiler.
     * @return the available Java compiler
     */
    protected JavaCompiler getJavaCompiler() {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        if (compiler == null) {
            throw new DiagnosticException(
                    com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                    "system Java compiler is not enabled");
        }
        return compiler;
    }

    /**
     * Returns whether compile is required or not.
     * @return {@code true} if compile is required, otherwise {@code false}
     */
    protected abstract boolean isCompileRequired();

    /**
     * Creates a configured {@link JavaFileManager}.
     * @param compiler the current Java compiler
     * @param listener the diagnostic listener
     * @return the created {@link JavaFileManager}
     * @throws IOException if failed to configure the
     */
    protected abstract JavaFileManager getJavaFileManager(
            JavaCompiler compiler,
            DiagnosticListener<JavaFileObject> listener) throws IOException;

    /**
     * Returns the source and target version.
     * @return the source and target version
     */
    protected String getCompliantVersion() {
        return DEFAULT_COMPLIANT_VERSION;
    }

    /**
     * Returns the extra Java compiler options.
     * @return the extra Java compiler options
     */
    protected List<String> getCompilerOptions() {
        return Collections.emptyList();
    }

    /**
     * Returns the annotation processor class names.
     * @return the annotation processor class names
     */
    protected List<String> getAnnotationProcessors() {
        return Collections.emptyList();
    }

    /**
     * Returns the compilation target source files.
     * @param fileManager the file manager to detecting for source files
     * @return the compilation target source files
     */
    protected abstract Iterable<? extends JavaFileObject> getSourceFiles(JavaFileManager fileManager);

    private void doCompile(JavaCompiler compiler) throws IOException {
        assert compiler != null;
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (JavaFileManager fileManager = getJavaFileManager(compiler, diagnostics)) {
            List<String> arguments = new ArrayList<>();
            String compliance = getCompliantVersion();
            if (compliance != null) {
                Collections.addAll(arguments, "-source", compliance); //$NON-NLS-1$
                Collections.addAll(arguments, "-target", compliance); //$NON-NLS-1$
            }
            Collections.addAll(arguments, "-encoding", getEncoding().name()); //$NON-NLS-1$
            arguments.addAll(getCompilerOptions());

            StringWriter errors = new StringWriter();
            boolean success;
            try (PrintWriter pw = new PrintWriter(errors)) {
                CompilationTask task;
                try {
                    LOG.debug("javac options: {}", arguments); //$NON-NLS-1$
                    task = compiler.getTask(
                            pw,
                            fileManager,
                            diagnostics,
                            arguments,
                            getAnnotationProcessors(),
                            getSourceFiles(fileManager));
                } catch (RuntimeException e) {
                    throw new DiagnosticException(
                            com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                            MessageFormat.format(
                                    "failed to initialize Java compiler: arguments={0}",
                                    arguments),
                            e);
                }
                success = task.call();
            }
            List<com.asakusafw.lang.compiler.common.Diagnostic> results = new ArrayList<>();
            for (Diagnostic<? extends JavaFileObject> diagnostic : diagnostics.getDiagnostics()) {
                dumpDiagnostic(diagnostic);
                switch (diagnostic.getKind()) {
                case ERROR:
                case MANDATORY_WARNING:
                    results.add(new BasicDiagnostic(
                            com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                            diagnostic.getMessage(null)));
                    break;
                case WARNING:
                    results.add(new BasicDiagnostic(
                            com.asakusafw.lang.compiler.common.Diagnostic.Level.WARN,
                            diagnostic.getMessage(null)));
                    break;
                default:
                    results.add(new BasicDiagnostic(
                            com.asakusafw.lang.compiler.common.Diagnostic.Level.INFO,
                            diagnostic.getMessage(null)));
                    break;
                }
            }
            if (success == false) {
                if (LOG.isWarnEnabled()) {
                    try (Scanner scanner = new Scanner(errors.toString())) {
                        while (scanner.hasNextLine()) {
                            LOG.warn(scanner.nextLine());
                        }
                    }
                }
                if (results.isEmpty()) {
                    // fatal error
                    throw new DiagnosticException(
                            com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                            errors.toString());
                } else {
                    throw new DiagnosticException(results);
                }
            }
        }
    }

    private void dumpDiagnostic(Diagnostic<? extends JavaFileObject> diagnostic) {
        if (LOG.isInfoEnabled()) {
            JavaFileObject source = diagnostic.getSource();
            try (Scanner scanner = new Scanner(source.openReader(true))) {
                LOG.info("== {}:{}", source.toUri(), diagnostic.getLineNumber()); //$NON-NLS-1$
                int lineNumber = 1;
                while (scanner.hasNextLine()) {
                    if (lineNumber == diagnostic.getLineNumber()) {
                        LOG.info("// >>>"); //$NON-NLS-1$
                        LOG.info(scanner.nextLine());
                        LOG.info("// <<<"); //$NON-NLS-1$
                    } else {
                        LOG.info(scanner.nextLine());
                    }
                    lineNumber++;
                }
                LOG.info("=="); //$NON-NLS-1$
                LOG.info("message: {}", diagnostic.getMessage(null)); //$NON-NLS-1$
            } catch (IOException e) {
                LOG.warn(MessageFormat.format(
                        "exception occurred while inspecting compile error: {0}",
                        source.toUri()), e);
            }
        }
    }
}
