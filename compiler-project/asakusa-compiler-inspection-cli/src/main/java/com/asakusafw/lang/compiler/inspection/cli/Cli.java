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
package com.asakusafw.lang.compiler.inspection.cli;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.TreeMap;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.inspection.InspectionNode;
import com.asakusafw.lang.compiler.inspection.InspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.json.JsonInspectionNodeRepository;
import com.asakusafw.lang.compiler.inspection.processor.StoreProcessor;
import com.asakusafw.lang.compiler.inspection.processor.DetailProcessor;
import com.asakusafw.lang.compiler.inspection.processor.DotProcessor;
import com.asakusafw.lang.compiler.inspection.processor.InspectionNodeProcessor;

/**
 * Processes inspection object.
 */
public final class Cli {

    static final Logger LOG = LoggerFactory.getLogger(Cli.class);

    private static final InspectionNodeProcessor DEFAULT_PROCESSOR = new DetailProcessor();

    private static final Map<String, InspectionNodeProcessor> BUILTIN_PROCESSORS;
    static {
        Map<String, InspectionNodeProcessor> map = new LinkedHashMap<>();
        map.put("txt", new DetailProcessor()); //$NON-NLS-1$
        map.put("json", new StoreProcessor()); //$NON-NLS-1$
        map.put("dot", new DotProcessor()); //$NON-NLS-1$
        BUILTIN_PROCESSORS = map;
    }

    private static final String PATH_SEPARATOR = "/";

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
                    Arrays.toString(args)), e);
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
                    "error occurred while processing inspection object: {0}",
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
        results.path = parseOpt(cmd, opts.path, false);
        results.format = parseOpt(cmd, opts.format, false);
        results.properties = parseProperties(cmd, opts.properties);
        return results;
    }

    private static File parseFile(CommandLine cmd, Option opt, boolean mandatory) {
        String value = parseOpt(cmd, opt, mandatory);
        if (value == null) {
            return null;
        }
        return new File(value);
    }

    private static Map<String, String> parseProperties(CommandLine cmd, Option option) {
        Properties properties = cmd.getOptionProperties(option.getLongOpt());
        Map<String, String> results = new TreeMap<>();
        for (Map.Entry<Object, Object> entry : properties.entrySet()) {
            results.put((String) entry.getKey(), (String) entry.getValue());
        }
        return results;
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

    static void process(Configuration configuration) throws IOException, ReflectiveOperationException {
        InspectionNodeProcessor processor = loadProcessor(configuration.format);
        InspectionNodeProcessor.Context context = new InspectionNodeProcessor.Context(configuration.properties);
        InspectionNode node = loadInput(configuration.input, configuration.repository, configuration.path);
        if (configuration.output == null) {
            processor.process(context, node, System.out);
        } else {
            try (OutputStream output = openOutput(configuration.output)) {
                processor.process(context, node, output);
            }
        }
    }

    private static InspectionNodeProcessor loadProcessor(String format) throws ReflectiveOperationException {
        if (format == null) {
            return DEFAULT_PROCESSOR;
        }
        InspectionNodeProcessor builtin = BUILTIN_PROCESSORS.get(format);
        if (builtin != null) {
            return builtin;
        }
        LOG.debug("loading processor: {}", format); //$NON-NLS-1$
        return Class.forName(format).asSubclass(InspectionNodeProcessor.class).newInstance();
    }

    private static InspectionNode loadInput(
            File file, InspectionNodeRepository repository, String path) throws IOException {
        InspectionNode node = loadInput(file, repository);
        if (path != null) {
            return find(node, path);
        }
        return node;
    }

    private static InspectionNode loadInput(File file, InspectionNodeRepository repository) throws IOException {
        LOG.debug("loading file: {}", file); //$NON-NLS-1$
        try (InputStream input = new FileInputStream(file)) {
            return repository.load(input);
        }
    }

    private static OutputStream openOutput(File file) throws IOException {
        File parent = file.getAbsoluteFile().getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create output: {0}",
                    file));
        }
        return new FileOutputStream(file);
    }

    private static InspectionNode find(InspectionNode node, String path) {
        InspectionNode current = node;
        StringBuilder history = new StringBuilder();
        for (String segment : path.split(PATH_SEPARATOR)) {
            if (segment.isEmpty()) {
                continue;
            }
            if (history.length() >= 1) {
                history.append(PATH_SEPARATOR);
            }
            history.append(segment);
            InspectionNode next = current.getElements().get(segment);
            if (next == null) {
                throw new NoSuchElementException(history.toString());
            }
            current = next;
        }
        return current;
    }

    private static class Opts {

        final Option input = required("input", 1) //$NON-NLS-1$
                .withDescription("input inspection file")
                .withArgumentDescription("/path/to/input-file.json");

        final Option path = optional("path", 1) //$NON-NLS-1$
                .withDescription("inspection path (default: root)")
                .withArgumentDescription("path/to/target-node");

        final Option output = optional("output", 1) //$NON-NLS-1$
                .withDescription("output file (default: stdout)")
                .withArgumentDescription("/path/to/output-file");

        final Option format = optional("format", 1) //$NON-NLS-1$
                .withDescription("output format (default: txt)")
                .withArgumentDescription("txt|dot|json|class-name");

        final Option properties = properties("P", "property") //$NON-NLS-1$ //$NON-NLS-2$
                .withDescription("format property")
                .withArgumentDescription("key=value");

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
            return option;
        }
    }

    static class Configuration {

        File input;

        File output;

        String path;

        String format;

        Map<String, String> properties;

        InspectionNodeRepository repository = new JsonInspectionNodeRepository();
    }
}
