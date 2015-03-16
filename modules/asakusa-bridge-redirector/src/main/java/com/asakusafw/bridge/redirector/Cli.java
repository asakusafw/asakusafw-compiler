package com.asakusafw.bridge.redirector;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CLI for API invocation redirector.
 *
 * <h3> Program Arguments </h3>
 * <dl>
 * <dt><code>--input &lt;/path/to/input-jar-file&gt;</code></dt>
 * <dd>the redirection target library JAR file</dd>
 *
 * <dt><code>--output &lt;/path/to/output-jar-file&gt;</code> <em>(optional)</em></dt>
 * <dd>the output library JAR file</dd>
 * <dd>default: <em>(replaces the original input JAR file)</em></dd>
 *
 * <dt><code>-R, --redirect &lt;source-class&gt;/&lt;destination-class&gt;</code></dt>
 * <dd>individual redirection rule</dd>
 *
 * <dt><code>--overwrite</code></dt>
 * <dd>do not create '.bak' files</dd>
 * </dl>
 */
public final class Cli {

    static final Logger LOG = LoggerFactory.getLogger(Cli.class);

    static final char RULE_VALUE_SEPARATOR = '/';

    static final String BACKUP_EXTENSION = ".bak"; //$NON-NLS-1$

    private Cli() {
        return;
    }

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
                            Cli.class.getName()),
                    new Opts().options,
                    true);
            return 1;
        }
        try {
            process(configuration);
        } catch (Exception e) {
            LOG.error(MessageFormat.format(
                    "error occurred while redirecting API invocations: {0}",
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
        results.input = parseFile(cmd, opts.input, true);
        results.output = parseFile(cmd, opts.output, false);
        results.rule = parseRules(cmd, opts.rule);
        results.overwrite = cmd.hasOption(opts.overwrite.getLongOpt());
        return results;
    }

    private static File parseFile(CommandLine cmd, Option opt, boolean mandatory) {
        String value = parseOpt(cmd, opt, mandatory);
        if (value == null) {
            return null;
        }
        return new File(value);
    }

    private static RedirectRule parseRules(CommandLine cmd, Option opt) {
        Properties ruleMap = cmd.getOptionProperties(opt.getLongOpt());
        RedirectRule rule = new RedirectRule();
        for (Map.Entry<Object, Object> entry : ruleMap.entrySet()) {
            assert entry.getKey() instanceof String;
            assert entry.getValue() instanceof String;
            String source = (String) entry.getKey();
            String destination = (String) entry.getValue();
            if (LOG.isDebugEnabled()) {
                LOG.debug("--{}: {} => {}", new Object[] { //$NON-NLS-1$
                        opt.getLongOpt(),
                        source,
                        destination
                });
            }
            rule.add(source, destination);
        }
        return rule;
    }

    private static String parseOpt(CommandLine cmd, Option opt, boolean mandatory) {
        String value = cmd.getOptionValue(opt.getLongOpt());
        if (value != null) {
            value = value.trim();
            if (value.isEmpty()) {
                value = null;
            }
        }
        LOG.debug("--{}: {}", opt.getLongOpt(), value); //$NON-NLS-1$
        if (value == null) {
            if (mandatory) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "option \"--{0}\" is mandatory",
                        opt.getLongOpt()));
            }
            return null;
        }
        return value;
    }

    static void process(Configuration configuration) throws IOException {
        File temporary = File.createTempFile("asakusa", "-redirect.jar"); //$NON-NLS-1$ //$NON-NLS-2$
        try {
            rewriteToFile(configuration.rule, configuration.input, temporary);

            File destination;
            if (configuration.output == null) {
                destination = configuration.input;
            } else {
                destination = configuration.output;
            }
            moveFile(temporary, destination, configuration.overwrite);
        } finally {
            if (deleteFile(temporary) == false) {
                LOG.warn(MessageFormat.format(
                        "failed to delete file: {0}",
                        temporary));
            }
        }
    }

    private static void rewriteToFile(RedirectRule rule, File input, File output) throws IOException {
        LOG.debug("analyzing file: {}->{}", input, output); //$NON-NLS-1$
        if (input.exists() == false) {
            throw new FileNotFoundException(input.getPath());
        }
        ZipRewriter rewriter = new ZipRewriter(rule);
        try (ZipInputStream in = new ZipInputStream(new FileInputStream(input));
                ZipOutputStream out = new ZipOutputStream(createFile(output))) {
            rewriter.rewrite(in, out);
        }
    }

    private static void moveFile(File source, File destination, boolean overwrite) throws IOException {
        if (overwrite == false) {
            escapeFile(destination);
        }
        try (InputStream input = new FileInputStream(source)) {
            try (OutputStream output = createFile(destination)) {
                Util.copy(input, output);
            }
        }
        if (deleteFile(source) == false) {
            LOG.warn(MessageFormat.format(
                    "failed to delete file: {0}",
                    source));
        }
    }

    private static OutputStream createFile(File file) throws IOException {
        File parent = file.getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create file: {0}",
                    file));
        }
        return new FileOutputStream(file);
    }

    private static boolean deleteFile(File file) {
        return file.delete() || file.exists() == false;
    }

    private static void escapeFile(File file) throws IOException {
        if (file.exists() == false) {
            return;
        }
        File escape = new File(file.getParentFile(), file.getName() + BACKUP_EXTENSION);
        LOG.debug("creating backup file: {}->{}", file, escape); //$NON-NLS-1$
        if (deleteFile(escape) == false) {
            throw new IOException(MessageFormat.format(
                    "failed to delete {0} file: {1}",
                    BACKUP_EXTENSION,
                    escape));
        }
        if (file.renameTo(escape) == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create {0} file: {1}",
                    BACKUP_EXTENSION,
                    file));
        }
    }

    private static class Opts {

        final Option input = required("input", 1) //$NON-NLS-1$
                .withDescription("redirection target library JAR file")
                .withArgumentDescription("/path/to/input-jar-file");

        final Option output = optional("output", 1) //$NON-NLS-1$
                .withDescription("redirection target library JAR file")
                .withArgumentDescription("/path/to/output-jar-file");

        final Option rule = properties("R", "rule") //$NON-NLS-1$ //$NON-NLS-2$
                .withValueSeparator(RULE_VALUE_SEPARATOR)
                .withDescription("individual redirection rule")
                .withArgumentDescription("source-class/destination-class");

        final Option overwrite = optional("overwrite", 0) //$NON-NLS-1$
                .withDescription("do not create '.bak' files");

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
            RichOption option = new RichOption(shortName, longName, 2, true);
            return option;
        }
    }

    static class Configuration {

        File input;

        File output;

        RedirectRule rule;

        boolean overwrite;
    }
}
