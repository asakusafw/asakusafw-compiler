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
package com.asakusafw.lang.info.cli;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;

/**
 * CLI entry for information models.
 * @since 0.4.2
 */
public final class Info {

    static final Logger LOG = LoggerFactory.getLogger(Info.class);

    static final String KEY_COMMAND_NAME = "info.cli.name";

    private Info() {
        return;
    }

    /**
     * Program entry.
     * @param args command line tokens
     */
    public static void main(String... args) {
        int status = exec(args);
        if (status != 0) {
            System.exit(status);
        }
    }

    static int exec(String... args) {
        String name = Optional.ofNullable(System.getProperty(KEY_COMMAND_NAME))
                .orElseGet(() -> Optional.ofNullable(findLibraryByClass(Info.class))
                        .map(it -> String.format("java -jar %s", it.getName()))
                        .orElse("java -jar <self.jar>"));
        Cli.CliBuilder<Runnable> builder = Cli.<Runnable>builder(name)
                .withDefaultCommand(Help.class)
                .withCommand(Help.class);

        builder.withGroup("list")
            .withDescription("Displays information list")
            .withDefaultCommand(ListUsageCommand.class)
            .withCommand(ListBatchCommand.class)
            .withCommand(ListParameterCommand.class)
            .withCommand(ListJobflowCommand.class)
            .withCommand(ListOperatorCommand.class)
            .withCommand(ListPlanCommand.class)
            .withCommand(ListDirectFileInputCommand.class)
            .withCommand(ListDirectFileOutputCommand.class)
            .withCommand(ListWindGateInputCommand.class)
            .withCommand(ListWindGateOutputCommand.class);

        builder.withGroup("draw")
            .withDescription("Generates Graphviz DOT scripts")
            .withDefaultCommand(DrawUsageCommand.class)
            .withCommand(DrawJobflowCommand.class)
            .withCommand(DrawOperatorCommand.class)
            .withCommand(DrawPlanCommand.class);

        Cli<Runnable> cli = builder.build();
        Runnable command;
        try {
            command = cli.parse(args);
        } catch (ParseException e) {
            LOG.error("Cannot recognize command, please type \"help\" to show command information: {}",
                    Arrays.toString(args),
                    e);
            return 1;
        }
        command.run();
        if (command instanceof BaseCommand) {
            switch (((BaseCommand) command).getStatus()) {
            case PREPARE_ERROR:
                return 1;
            case PROCESS_ERROR:
                return 2;
            case SUCCESS:
                return 0;
            default:
                return -1;
            }
        } else {
            return 0;
        }
    }

    static File findLibraryByClass(Class<?> aClass) {
        URL resource = toUrl(aClass);
        if (resource == null) {
            LOG.warn(MessageFormat.format(
                    "failed to locate the class file: {0}",
                    aClass.getName()));
            return null;
        }
        String resourcePath = aClass.getName().replace('.', '/') + ".class";
        return findLibraryFromUrl(resource, resourcePath);
    }

    private static URL toUrl(Class<?> aClass) {
        String className = aClass.getName();
        int start = className.lastIndexOf('.') + 1;
        String name = className.substring(start);
        URL resource = aClass.getResource(name + ".class");
        return resource;
    }

    private static File findLibraryFromUrl(URL resource, String resourcePath) {
        assert resource != null;
        assert resourcePath != null;
        String protocol = resource.getProtocol();
        if (protocol.equals("file")) { //$NON-NLS-1$
            try {
                File file = new File(resource.toURI());
                return toClassPathRoot(file, resourcePath);
            } catch (URISyntaxException e) {
                LOG.warn(MessageFormat.format(
                        "failed to locate the library path (cannot convert to local file): {0}",
                        resource), e);
                return null;
            }
        }
        if (protocol.equals("jar")) { //$NON-NLS-1$
            String path = resource.getPath();
            return toClassPathRoot(path, resourcePath);
        } else {
            LOG.warn(MessageFormat.format(
                    "failed to locate the library path (unsupported protocol {0}): {1}",
                    resource,
                    resourcePath));
            return null;
        }
    }

    private static File toClassPathRoot(File resourceFile, String resourcePath) {
        assert resourceFile != null;
        assert resourcePath != null;
        assert resourceFile.isFile();
        File current = resourceFile.getParentFile();
        assert current != null && current.isDirectory() : resourceFile;
        for (int start = resourcePath.indexOf('/'); start >= 0; start = resourcePath.indexOf('/', start + 1)) {
            current = current.getParentFile();
            if (current == null || current.isDirectory() == false) {
                LOG.warn(MessageFormat.format(
                        "failed to locate the library path: {0} ({1})",
                        resourceFile,
                        resourcePath));
                return null;
            }
        }
        return current;
    }

    private static File toClassPathRoot(String uriQualifiedPath, String resourceName) {
        assert uriQualifiedPath != null;
        assert resourceName != null;
        int entry = uriQualifiedPath.lastIndexOf('!');
        String qualifier;
        if (entry >= 0) {
            qualifier = uriQualifiedPath.substring(0, entry);
        } else {
            qualifier = uriQualifiedPath;
        }
        URI archive;
        try {
            archive = new URI(qualifier);
        } catch (URISyntaxException e) {
            LOG.warn(MessageFormat.format(
                    "failed to locate the JAR library file {0}: {1}",
                    qualifier,
                    resourceName),
                    e);
            throw new UnsupportedOperationException(qualifier, e);
        }
        if (archive.getScheme().equals("file") == false) { //$NON-NLS-1$
            LOG.warn(MessageFormat.format(
                    "failed to locate the library path (unsupported protocol {0}): {1}",
                    archive,
                    resourceName));
            return null;
        }
        File file = new File(archive);
        assert file.isFile() : file;
        return file;
    }
}
