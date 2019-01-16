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
package com.asakusafw.dag.iterative;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.launch.AbstractFileOption;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;
import com.asakusafw.lang.utils.common.Arguments;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

/**
 * Parses JSON style {@link ParameterTable}.
 * @since 0.4.1
 */
public class DirectParameterTableOption extends AbstractFileOption<ParameterTable> {

    static final Logger LOG = LoggerFactory.getLogger(DirectParameterTableOption.class);

    private final Set<String> commandNames;

    private ParameterTable result;

    /**
     * Creates a new instance.
     * @param commandNames the command names
     */
    public DirectParameterTableOption(String... commandNames) {
        this.commandNames = Arguments.freezeToSet(commandNames);
    }

    @Override
    public Set<String> getCommands() {
        return commandNames;
    }

    @Override
    protected void accept(String command, File value) throws LaunchConfigurationException {
        if (result != null) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "duplicate launch option: {0}",
                    command));
        }
        LOG.debug("extracting parameter table: {}", value); //$NON-NLS-1$
        try {
            result = parse(value);
        } catch (IOException e) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "error occurred while extracting parameter table ({0}): {1}",
                    command, value), e);
        }
    }

    @Override
    public ParameterTable resolve() throws LaunchConfigurationException {
        return result;
    }

    /**
     * Parses the given JSON file and generates {@link ParameterTable}.
     * @param file the target JSON file
     * @return the parsed {@link ParameterTable}
     * @throws IOException if I/O error was occurred while parsing the given file
     */
    public static ParameterTable parse(File file) throws IOException {
        LOG.debug("parsing JSON parameter table: {}", file); //$NON-NLS-1$
        JsonFactory json = new JsonFactory();
        json.disable(JsonParser.Feature.AUTO_CLOSE_SOURCE);
        json.enable(JsonParser.Feature.ALLOW_COMMENTS);
        json.enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES);
        json.enable(JsonParser.Feature.ALLOW_SINGLE_QUOTES);
        json.enable(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER);
        json.enable(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS);
        try (JsonParser parser = json.createParser(file)) {
            ParameterTable.Builder builder = IterativeExtensions.builder();
            while (true) {
                JsonToken t = parser.nextToken();
                if (t == null) {
                    break;
                } else if (t == JsonToken.START_OBJECT) {
                    builder.next();
                    parseRow(parser, builder);
                } else {
                    throw new IOException(MessageFormat.format(
                            "invalid JSON format (invalid start object): {0}",
                            parser.getCurrentLocation()));
                }
            }
            return builder.build();
        }
    }

    private static void parseRow(JsonParser parser, ParameterTable.Builder builder) throws IOException {
        while (true) {
            JsonToken t0 = parser.nextToken();
            if (t0 == JsonToken.END_OBJECT) {
                return;
            } else if (t0 != JsonToken.FIELD_NAME) {
                throw new IOException(MessageFormat.format(
                        "invalid JSON format (invalid field name): {0}",
                        parser.getCurrentLocation()));
            }
            String name = parser.getCurrentName();

            JsonToken t1 = parser.nextToken();
            if (t1 == null) {
                throw new IOException(MessageFormat.format(
                        "invalid JSON format (unexpected EOF): {0}",
                        parser.getCurrentLocation()));
            }
            switch (t1) {
            case VALUE_STRING:
            case VALUE_NUMBER_FLOAT:
            case VALUE_NUMBER_INT:
            case VALUE_TRUE:
            case VALUE_FALSE:
                // ok
                break;
            default:
                throw new IOException(MessageFormat.format(
                        "invalid JSON format (unsupported value): {0}",
                        parser.getCurrentLocation()));
            }
            String value = parser.getValueAsString();

            builder.put(name, value);
        }
    }
}
