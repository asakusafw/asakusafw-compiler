package com.asakusafw.lang.compiler.extension.javac;

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

    private Location toLocation(ClassDescription aClass) {
        Location location = Location.of(aClass.getName(), '.');
        location = new Location(location.getParent(), location.getName() + JAVA_EXTENSION);
        return location;
    }

    @Override
    public final void process(Context context) throws IOException {
        if (isCompileRequired(context) == false) {
            return;
        }
        JavaCompiler compiler = getJavaCompiler(context);
        try {
            doCompile(context, compiler);
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
     * @param context the current context
     * @return the available Java compiler
     */
    protected JavaCompiler getJavaCompiler(Context context) {
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
     * @param context the current context
     * @return {@code true} if compile is required, otherwise {@code false}
     */
    protected abstract boolean isCompileRequired(Context context);

    /**
     * Creates a configured {@link JavaFileManager}.
     * @param context the current context
     * @param compiler the current Java compiler
     * @param listener the diagnostic listener
     * @return the created {@link JavaFileManager}
     * @throws IOException if failed to configure the
     */
    protected abstract JavaFileManager getJavaFileManager(
            Context context,
            JavaCompiler compiler,
            DiagnosticListener<JavaFileObject> listener) throws IOException;

    /**
     * Returns the source and target version.
     * @param context the current context
     * @return the source and target version
     */
    protected String getCompliantVersion(Context context) {
        return DEFAULT_COMPLIANT_VERSION;
    }

    /**
     * Returns the extra Java compiler options.
     * @param context the current context
     * @return the extra Java compiler options
     */
    protected List<String> getCompilerOptions(Context context) {
        return Collections.emptyList();
    }

    /**
     * Returns the annotation processor class names.
     * @param context the current context
     * @return the annotation processor class names
     */
    protected List<String> getAnnotationProcessors(Context context) {
        return Collections.emptyList();
    }

    /**
     * Returns the compilation target source files.
     * @param context the current context
     * @param fileManager the file manager to detecting for source files
     * @return the compilation target source files
     */
    protected abstract Iterable<? extends JavaFileObject> getSourceFiles(Context context, JavaFileManager fileManager);

    private void doCompile(Context context, JavaCompiler compiler) throws IOException {
        assert compiler != null;
        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        try (JavaFileManager fileManager = getJavaFileManager(context, compiler, diagnostics)) {
            List<String> arguments = new ArrayList<>();
            String compliance = getCompliantVersion(context);
            if (compliance != null) {
                Collections.addAll(arguments, "-source", compliance); //$NON-NLS-1$
                Collections.addAll(arguments, "-target", compliance); //$NON-NLS-1$
            }
            Collections.addAll(arguments, "-encoding", getEncoding().name()); //$NON-NLS-1$
            arguments.addAll(getCompilerOptions(context));

            StringWriter errors = new StringWriter();
            PrintWriter pw = new PrintWriter(errors);

            CompilationTask task;
            try {
                task = compiler.getTask(
                        pw,
                        fileManager,
                        diagnostics,
                        arguments,
                        getAnnotationProcessors(context),
                        getSourceFiles(context, fileManager));
            } catch (RuntimeException e) {
                throw new DiagnosticException(
                        com.asakusafw.lang.compiler.common.Diagnostic.Level.ERROR,
                        MessageFormat.format(
                                "failed to initialize Java compiler: arguments={0}",
                                arguments),
                        e);
            }

            boolean success = task.call();
            pw.close();
            List<com.asakusafw.lang.compiler.common.Diagnostic> results = new ArrayList<>();
            for (Diagnostic<?> diagnostic : diagnostics.getDiagnostics()) {
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
}
