package com.asakusafw.lang.compiler.cli;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URLClassLoader;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.BatchProcessor;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.api.DataModelProcessor;
import com.asakusafw.lang.compiler.api.ExternalPortProcessor;
import com.asakusafw.lang.compiler.api.JobflowProcessor;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Predicate;
import com.asakusafw.lang.compiler.common.Predicates;
import com.asakusafw.lang.compiler.core.BatchCompiler;
import com.asakusafw.lang.compiler.core.ClassAnalyzer;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.CompilerParticipant;
import com.asakusafw.lang.compiler.core.ProjectRepository;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.core.basic.BasicBatchCompiler;
import com.asakusafw.lang.compiler.core.basic.BasicClassAnalyzer;
import com.asakusafw.lang.compiler.core.basic.JobflowPackager;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Command line interface for {@link BatchCompiler}.
 *
 * <h3> Program Arguments </h3>
 * <dl>
 * <dt><code>--explore &lt;/path/to/lib1[:/path/to/lib2[:..]]&gt;</code></dt>
 * <dd>library paths with batch classes</dd>
 *
 * <dt><code>--output &lt;/path/to/output&gt;</code></dt>
 * <dd>output directory</dd>
 *
 * <dt><code>--attach &lt;/path/to/lib1[:/path/to/lib2[:..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>library paths to be attached to each batch package</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--embed &lt;/path/to/lib1[:/path/to/lib2[:..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>library paths to be embedded to each jobflow package</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--external &lt;/path/to/lib1[:/path/to/lib2[:..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>external library paths</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--include &lt;class-name-pattern&gt;</code> <em>(optional)</em></dt>
 * <dd>accepting batch class name pattern ({@code "*"} as a wildcard character)</dd>
 * <dd>default: <em>(accepts anything)</em></dd>
 *
 * <dt><code>--exclude &lt;class-name-pattern&gt;</code> <em>(optional)</em></dt>
 * <dd>denying batch class name pattern ({@code "*"} as a wildcard character)</dd>
 * <dd>default: <em>(denies nothing)</em></dd>
 *
 * <dt><code>--dataModelProcessors &lt;class-name1[,class-name2[,..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>custom data model processor classes</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--externalPortProcessors &lt;class-name1[,class-name2[,..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>custom external port processor classes</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--jobflowProcessors &lt;class-name1[,class-name2[,..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>custom jobflow processor classes</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--batchProcessors &lt;class-name1[,class-name2[,..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>custom batch processor classes</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--participants &lt;class-name1[,class-name2[,..]]&gt;</code> <em>(optional)</em></dt>
 * <dd>custom compiler participant classes</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 *
 * <dt><code>--runtimeWorkingDirectory &lt;path/to/working&gt;</code> <em>(optional)</em></dt>
 * <dd>custom runtime working directory path</dd>
 * <dd>default: {@link #DEFAULT_RUNTIME_WORKING_DIRECTORY}</dd>
 *
 * <dt><code>-P,--property &lt;key=value&gt;</code> <em>(optional)</em></dt>
 * <dd>compiler property</dd>
 * <dd>default: <em>(empty)</em></dd>
 *
 * <dt><code>--failOnError</code> <em>(optional)</em></dt>
 * <dd>whether fails on compilation errors or not</dd>
 * <dd>default: <em>(skip on errors)</em></dd>
 *
 *
 * <dt><code>--batchCompiler &lt;class-name&gt;</code> <em>(optional)</em></dt>
 * <dd>custom batch compiler class</dd>
 * <dd>default: {@link BasicBatchCompiler}</dd>
 *
 * <dt><code>--classAnalyzer &lt;class-name&gt;</code> <em>(optional)</em></dt>
 * <dd>custom class analyzer class</dd>
 * <dd>default: {@link BasicClassAnalyzer}</dd>
 *
 * </dl>
 */
public class BatchCompilerCli {

    static final String CLASS_SEPARATOR = ","; //$NON-NLS-1$

    static final ClassDescription DEFAULT_BATCH_COMPILER = Descriptions.classOf(BasicBatchCompiler.class);

    static final ClassDescription DEFAULT_CLASS_ANALYZER = Descriptions.classOf(BasicClassAnalyzer.class);

    /**
     * The default runtime working directory.
     */
    public static final String DEFAULT_RUNTIME_WORKING_DIRECTORY = "target/hadoopwork"; //$NON-NLS-1$

    static final Logger LOG = LoggerFactory.getLogger(BatchCompilerCli.class);

    /**
     * The program entry.
     * @param args application arguments
     * @throws Exception if failed
     */
    public static void main(String[] args) throws Exception {
        int status = execute(args);
        if (status != 0) {
            System.exit(status);
        }
    }

    /**
     * The program entry.
     * @param args application arguments
     * @return the exit code
     */
    public static int execute(String... args) {
        Configuration configuration;
        try {
            configuration = parse(args);
        } catch (Exception e) {
            LOG.error(MessageFormat.format(
                    "error occurred while analyzing arguments: {0}",
                    (Object) args), e);
            HelpFormatter formatter = new HelpFormatter();
            formatter.setWidth(Integer.MAX_VALUE);
            formatter.printHelp(
                    MessageFormat.format(
                            "java -classpath ... {0}", //$NON-NLS-1$
                            BatchCompilerCli.class.getName()),
                    new Opts().options,
                    true);
            return 1;
        }
        try {
            if (process(configuration) == false) {
                return 1;
            }
        } catch (Exception e) {
            LOG.error(MessageFormat.format(
                    "error occurred while compiling batch classes: {0}",
                    Arrays.toString(args)), e);
            return 1;
        }
        return 0;
    }

    static Configuration parse(String... args) throws ParseException {
        LOG.debug("analyzing command line arguments: {}", Arrays.toString(args)); //$NON-NLS-1$

        Opts opts = new Opts();
        CommandLineParser parser = new BasicParser();
        CommandLine cmd = parser.parse(opts.options, args);

        Configuration results = new Configuration();
        results.classAnalyzer.set(parseClass(cmd, opts.classAnalyzer));
        results.batchCompiler.set(parseClass(cmd, opts.batchCompiler));
        results.output.set(parseFile(cmd, opts.output, false));
        results.external.addAll(parseFiles(cmd, opts.external, true));
        results.explore.addAll(parseFiles(cmd, opts.explore, true));
        results.embed.addAll(parseFiles(cmd, opts.embed, true));
        results.attach.addAll(parseFiles(cmd, opts.attach, true));
        results.dataModelProcessors.addAll(parseClasses(cmd, opts.dataModelProcessors));
        results.externalPortProcessors.addAll(parseClasses(cmd, opts.externalPortProcessors));
        results.batchProcessors.addAll(parseClasses(cmd, opts.batchProcessors));
        results.jobflowProcessors.addAll(parseClasses(cmd, opts.jobflowProcessors));
        results.compilerParticipants.addAll(parseClasses(cmd, opts.compilerParticipants));
        results.sourcePredicate.add(parsePattern(cmd, opts.include, false));
        results.sourcePredicate.add(parsePattern(cmd, opts.exclude, true));
        results.runtimeWorkingDirectory.set(parse(cmd, opts.runtimeWorkingDirectory));
        results.properties.putAll(parseProperties(cmd, opts.properties));
        results.failOnError.set(cmd.hasOption(opts.failOnError.getLongOpt()));
        return results;
    }

    private static String parse(CommandLine cmd, Option option) {
        String value = cmd.getOptionValue(option.getLongOpt());
        if (value != null && value.isEmpty()) {
            value = null;
        }
        if (value == null && option.isRequired()) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "required argument was not set: --{0}",
                    option.getLongOpt()));
        }
        LOG.debug("--{}: {}", option.getLongOpt(), value); //$NON-NLS-1$
        return value;
    }

    private static Predicate<? super Class<?>> parsePattern(CommandLine cmd, Option option, boolean negate) {
        String value = parse(cmd, option);
        if (value == null) {
            return null;
        }
        Predicate<? super Class<?>> resolved = resolvePattern(option, value);
        if (negate) {
            resolved = Predicates.not(resolved);
        }
        return resolved;
    }

    private static File parseFile(CommandLine cmd, Option option, boolean check) {
        String value = parse(cmd, option);
        if (value == null) {
            return null;
        }
        return resolveFile(option, value, check);
    }

    private static List<File> parseFiles(CommandLine cmd, Option option, boolean check) {
        String value = parse(cmd, option);
        if (value == null) {
            return Collections.emptyList();
        }
        List<File> results = new ArrayList<>();
        for (String segment : value.split(Pattern.quote(File.pathSeparator))) {
            String s = segment.trim();
            if (s.isEmpty()) {
                continue;
            }
            results.add(resolveFile(option, s, check));
        }
        return results;
    }

    private static ClassDescription parseClass(CommandLine cmd, Option option) {
        String value = parse(cmd, option);
        if (value == null) {
            return null;
        }
        return resolveClass(option, value);
    }

    private static List<ClassDescription> parseClasses(CommandLine cmd, Option option) {
        String value = parse(cmd, option);
        if (value == null) {
            return Collections.emptyList();
        }
        List<ClassDescription> results = new ArrayList<>();
        for (String segment : value.split(CLASS_SEPARATOR)) {
            String s = segment.trim();
            if (s.isEmpty()) {
                continue;
            }
            results.add(resolveClass(option, s));
        }
        return results;
    }

    private static Map<String, String> parseProperties(CommandLine cmd, Option option) {
        Properties properties = cmd.getOptionProperties(option.getLongOpt());
        Map<String, String> results = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            results.put((String) entry.getKey(), (String) entry.getValue());
        }
        return results;
    }

    private static Predicate<? super Class<?>> resolvePattern(Option option, String value) {
        return ClassNamePredicate.parse(value);
    }

    private static File resolveFile(Option output, String value, boolean check) {
        File result = new File(value);
        if (check && result.exists() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "missing file: {1} (--{0})",
                    output.getLongOpt(),
                    result));
        }
        return result;
    }

    private static ClassDescription resolveClass(Option option, String value) {
        return new ClassDescription(value);
    }

    static boolean process(Configuration configuration) throws IOException {
        CompilerOptions options = loadOptions(configuration);
        FileContainerRepository temporary = loadTemporary(configuration);
        try (ProjectRepository project = loadProject(configuration)) {
            ToolRepository tools = loadTools(project.getClassLoader(), configuration);
            CompilerContext context = new CompilerContext.Basic(options, project, tools, temporary);
            report(context);
            return process(context, configuration);
        } finally {
            temporary.reset();
        }
    }

    private static void report(CompilerContext context) {
        if (LOG.isDebugEnabled() == false) {
            return;
        }
        LOG.debug("project info:"); //$NON-NLS-1$
        LOG.debug("   project: {}", context.getProject().getProjectContents()); //$NON-NLS-1$
        LOG.debug("  embedded: {}", context.getProject().getEmbeddedContents()); //$NON-NLS-1$
        LOG.debug("  attached: {}", context.getProject().getAttachedLibraries()); //$NON-NLS-1$
        LOG.debug("tools info:"); //$NON-NLS-1$
        LOG.debug("   data model: {}", context.getTools().getDataModelProcessor()); //$NON-NLS-1$
        LOG.debug("     external: {}", context.getTools().getExternalPortProcessor()); //$NON-NLS-1$
        LOG.debug("        batch: {}", context.getTools().getBatchProcessor()); //$NON-NLS-1$
        LOG.debug("      jobflow: {}", context.getTools().getJobflowProcessor()); //$NON-NLS-1$
        LOG.debug("  participant: {}", context.getTools().getParticipant()); //$NON-NLS-1$
    }

    private static CompilerOptions loadOptions(Configuration configuration) {
        return CompilerOptions.builder()
                .withRuntimeWorkingDirectory(configuration.runtimeWorkingDirectory.get(), true)
                .withProperties(configuration.properties)
                .build();
    }

    private static FileContainerRepository loadTemporary(Configuration configuration) throws IOException {
        File temporary = File.createTempFile("asakusa", ".tmp"); //$NON-NLS-1$ //$NON-NLS-2$
        if (temporary.delete() == false || temporary.mkdirs() == false) {
            throw new IOException("failed to create a compiler temporary working directory");
        }
        return new FileContainerRepository(temporary);
    }

    private static ProjectRepository loadProject(Configuration configuration) throws IOException {
        ProjectRepository.Builder builder = ProjectRepository.builder(BatchCompilerCli.class.getClassLoader());
        for (File file : configuration.explore) {
            builder.explore(file);
            builder.embed(file);
        }
        for (File file : configuration.embed) {
            builder.embed(file);
        }
        for (File file : configuration.attach) {
            builder.attach(file);
        }
        for (File file : configuration.external) {
            builder.external(file);
        }
        try (URLClassLoader loader = builder.buildClassLoader()) {
            Set<File> marked = ResourceUtil.findLibrariesByResource(loader, JobflowPackager.FRAGMENT_MARKER);
            for (File file : marked) {
                builder.embed(file);
            }
        }
        return builder.build();
    }

    private static ToolRepository loadTools(ClassLoader classLoader, Configuration configuration) {
        ToolRepository.Builder builder = ToolRepository.builder(classLoader);
        for (ClassDescription aClass : configuration.dataModelProcessors) {
            builder.use(newInstance(classLoader, DataModelProcessor.class, aClass));
        }
        builder.useDefaults(DataModelProcessor.class);
        for (ClassDescription aClass : configuration.externalPortProcessors) {
            builder.use(newInstance(classLoader, ExternalPortProcessor.class, aClass));
        }
        builder.useDefaults(ExternalPortProcessor.class);
        for (ClassDescription aClass : configuration.batchProcessors) {
            builder.use(newInstance(classLoader, BatchProcessor.class, aClass));
        }
        builder.useDefaults(BatchProcessor.class);
        for (ClassDescription aClass : configuration.jobflowProcessors) {
            builder.use(newInstance(classLoader, JobflowProcessor.class, aClass));
        }
        builder.useDefaults(JobflowProcessor.class);
        for (ClassDescription aClass : configuration.compilerParticipants) {
            builder.use(newInstance(classLoader, CompilerParticipant.class, aClass));
        }
        builder.useDefaults(CompilerParticipant.class);
        return builder.build();
    }

    private static <T> T newInstance(ClassLoader classLoader, Class<T> type, ClassDescription aClass) {
        try {
            Class<?> resolved = aClass.resolve(classLoader);
            if (type.isAssignableFrom(resolved) == false) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "{0} must be a subtype of {1}",
                        type.getName(),
                        aClass.getName()));
            }
            return resolved.asSubclass(type).newInstance();
        } catch (ReflectiveOperationException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to instanciate a class: {0}",
                    aClass.getName()), e);
        }
    }

    private static boolean process(CompilerContext root, Configuration configuration) throws IOException {
        ClassLoader classLoader = root.getProject().getClassLoader();
        ClassAnalyzer analyzer = newInstance(classLoader, ClassAnalyzer.class, configuration.classAnalyzer.get());
        BatchCompiler compiler = newInstance(classLoader, BatchCompiler.class, configuration.batchCompiler.get());
        if (LOG.isDebugEnabled()) {
            LOG.debug("engines info:"); //$NON-NLS-1$
            LOG.debug("  analyzer: {}", analyzer.getClass().getName()); //$NON-NLS-1$
            LOG.debug("  compiler: {}", compiler.getClass().getName()); //$NON-NLS-1$
        }
        Predicate<? super Class<?>> predicate = loadPredicate(root, configuration, analyzer);
        boolean sawError = false;
        for (Class<?> aClass : root.getProject().getProjectClasses(predicate)) {
            if (LOG.isInfoEnabled()) {
                LOG.info(MessageFormat.format(
                        "compiling batch class: {0}",
                        aClass.getName()));
            }
            try {
                Batch batch = analyzer.analyzeBatch(new ClassAnalyzer.Context(root), aClass);
                File output = new File(configuration.output.get(), batch.getBatchId());
                if (output.exists()) {
                    LOG.debug("cleaning output target: {}", output); //$NON-NLS-1$
                    if (ResourceUtil.delete(output) == false) {
                        throw new IOException(MessageFormat.format(
                                "failed to delete output target: {0}",
                                output));
                    }
                }
                BatchCompiler.Context context = new BatchCompiler.Context(root, new FileContainer(output));
                compiler.compile(context, batch);
            } catch (DiagnosticException e) {
                sawError = true;
                for (Diagnostic diagnostics : e.getDiagnostics()) {
                    switch (diagnostics.getLevel()) {
                    case ERROR:
                        LOG.error(diagnostics.getMessage());
                        break;
                    case WARN:
                        LOG.warn(diagnostics.getMessage());
                        break;
                    case INFO:
                        LOG.info(diagnostics.getMessage());
                        break;
                    default:
                        throw new AssertionError(diagnostics);
                    }
                }
                if (configuration.failOnError.get()) {
                    throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                            "error occurred while compiling batch: {0}",
                            aClass.getName()));
                }
            }
        }
        return sawError == false;
    }

    private static Predicate<? super Class<?>> loadPredicate(
            CompilerContext root, Configuration configuration, final ClassAnalyzer analyzer) {
        final ClassAnalyzer.Context context = new ClassAnalyzer.Context(root);
        Predicate<Class<?>> predicate = new Predicate<Class<?>>() {
            @Override
            public boolean apply(Class<?> argument) {
                return analyzer.isBatchClass(context, argument);
            }
        };
        for (Predicate<? super Class<?>> p : configuration.sourcePredicate) {
            predicate = Predicates.and(predicate, p);
        }
        return predicate;
    }

    private static class Opts {

        private static final String ARG_CLASS_PATTERN = "class-name-pattern"; //$NON-NLS-1$

        private static final String ARG_CLASS = "class-name"; //$NON-NLS-1$

        private static final String ARG_CLASSES = "class-name1[,class-name2[,..]]"; //$NON-NLS-1$

        private static final String ARG_LIBRARIES = files("/path/to/lib1", "/path/to/lib2"); //$NON-NLS-1$ //$NON-NLS-2$

        final Option classAnalyzer = optional("classAnalyzer", 1) //$NON-NLS-1$
                .withDescription("custom class analyzer class")
                .withArgumentDescription(ARG_CLASS);

        final Option batchCompiler = optional("batchCompiler", 1) //$NON-NLS-1$
                .withDescription("custom batch compiler class")
                .withArgumentDescription(ARG_CLASS);

        final Option output = required("output", 1) //$NON-NLS-1$
                .withDescription("output directory")
                .withArgumentDescription("/path/to/output"); //$NON-NLS-1$

        final Option external = optional("external", 1) //$NON-NLS-1$
                .withDescription("external library paths")
                .withArgumentDescription(ARG_LIBRARIES);

        final Option explore = required("explore", 1) //$NON-NLS-1$
                .withDescription("library paths with batch classes")
                .withArgumentDescription(ARG_LIBRARIES);

        final Option embed = optional("embed", 1) //$NON-NLS-1$
                .withDescription("library paths to be embedded to each jobflow package")
                .withArgumentDescription(ARG_LIBRARIES);

        final Option attach = optional("attach", 1) //$NON-NLS-1$
                .withDescription("library paths to be attached to each batch package")
                .withArgumentDescription(ARG_LIBRARIES);

        final Option include = optional("include", 1) //$NON-NLS-1$
                .withDescription("included batch class name pattern")
                .withArgumentDescription(ARG_CLASS_PATTERN);

        final Option exclude = optional("exclude", 1) //$NON-NLS-1$
                .withDescription("excluded batch class name pattern")
                .withArgumentDescription(ARG_CLASS_PATTERN);

        final Option dataModelProcessors = optional("dataModelProcessors", 1) //$NON-NLS-1$
                .withDescription("custom data model processor classes")
                .withArgumentDescription(ARG_CLASSES);

        final Option externalPortProcessors = optional("externalPortProcessors", 1) //$NON-NLS-1$
                .withDescription("custom external port processor classes")
                .withArgumentDescription(ARG_CLASSES);

        final Option batchProcessors = optional("batchProcessors", 1) //$NON-NLS-1$
                .withDescription("custom batch processor classes")
                .withArgumentDescription(ARG_CLASSES);

        final Option jobflowProcessors = optional("jobflowProcessors", 1) //$NON-NLS-1$
                .withDescription("custom jobflow processor classes")
                .withArgumentDescription(ARG_CLASSES);

        final Option compilerParticipants = optional("participants", 1) //$NON-NLS-1$
                .withDescription("custom compiler participant classes")
                .withArgumentDescription(ARG_CLASSES);

        final Option runtimeWorkingDirectory = optional("runtimeWorkingDirectory", 1) //$NON-NLS-1$
                .withDescription("custom runtime working directory path")
                .withArgumentDescription("path/to/working"); //$NON-NLS-1$

        final Option properties = properties("P", "property") //$NON-NLS-1$ //$NON-NLS-2$
                .withDescription("compiler property")
                .withArgumentDescription("key=value"); //$NON-NLS-1$

        final Option failOnError = optional("failOnError", 0) //$NON-NLS-1$
                .withDescription("whether fails on compilation errors or not");

        final Options options = new Options();

        Opts() {
            for (Field field : Opts.class.getDeclaredFields()) {
                if (Option.class.isAssignableFrom(field.getType()) == false) {
                    continue;
                }
                try {
                    Option option = (Option) field.get(this);
                    options.addOption(option);
                } catch (Exception e) {
                    throw new IllegalStateException(e);
                }
            }
        }

        private static RichOption optional(String name, int arguments) {
            return new RichOption(null, name, arguments, false);
        }

        private static RichOption required(String name, int arguments) {
            return new RichOption(null, name, arguments, true);
        }

        private static RichOption properties(String shortName, String longName) {
            RichOption option = new RichOption(shortName, longName, 2, false);
            option.setValueSeparator('=');
            return option;
        }

        private static String files(String a, String b) {
            return elements(a, b, File.pathSeparator);
        }

        private static String elements(String a, String b, String separator) {
            return String.format("%s[%s%s[%s..]]", a, separator, b, separator); //$NON-NLS-1$
        }
    }

    static class Configuration {

        final ValueHolder<ClassDescription> classAnalyzer = new ValueHolder<>(DEFAULT_CLASS_ANALYZER);

        final ValueHolder<ClassDescription> batchCompiler = new ValueHolder<>(DEFAULT_BATCH_COMPILER);

        final ValueHolder<File> output = new ValueHolder<>();

        final ListHolder<File> external = new ListHolder<>();

        final ListHolder<File> explore = new ListHolder<>();

        final ListHolder<File> embed = new ListHolder<>();

        final ListHolder<File> attach = new ListHolder<>();

        final ListHolder<ClassDescription> dataModelProcessors = new ListHolder<>();

        final ListHolder<ClassDescription> externalPortProcessors = new ListHolder<>();

        final ListHolder<ClassDescription> batchProcessors = new ListHolder<>();

        final ListHolder<ClassDescription> jobflowProcessors = new ListHolder<>();

        final ListHolder<ClassDescription> compilerParticipants = new ListHolder<>();

        final ListHolder<Predicate<? super Class<?>>> sourcePredicate = new ListHolder<>();

        final ValueHolder<String> runtimeWorkingDirectory = new ValueHolder<>(DEFAULT_RUNTIME_WORKING_DIRECTORY);

        final Map<String, String> properties = new LinkedHashMap<>();

        final ValueHolder<Boolean> failOnError = new ValueHolder<>(Boolean.FALSE);

        Configuration() {
            return;
        }
    }
}
