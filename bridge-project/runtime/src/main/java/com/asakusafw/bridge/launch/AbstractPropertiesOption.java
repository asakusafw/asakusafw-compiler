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
package com.asakusafw.bridge.launch;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An abstract {@link LaunchOption} which accepts properties.
 */
public abstract class AbstractPropertiesOption implements LaunchOption<Map<String, String>> {

    /**
     * The prefix of file path.
     */
    public static final char FILE_PREFIX = '@';

    /**
     * The selective file separator.
     */
    public static final char FILE_SEPARATOR = '|';

    /**
     * The key-value separator character.
     */
    public static final char KEY_VALUE_SEPARATOR = '=';

    /**
     * The default value for key-only entries.
     */
    public static final String DEFAULT_VALUE = "true"; //$NON-NLS-1$

    private final Map<String, String> properties = new LinkedHashMap<>();

    @Override
    public void accept(String command, String value) throws LaunchConfigurationException {
        if (value.isEmpty()) {
            return;
        }
        if (value.charAt(0) == FILE_PREFIX) {
            for (String s : value.substring(1).split(Pattern.quote(String.valueOf(FILE_SEPARATOR)))) {
                if (s.isEmpty()) {
                    continue;
                }
                File file = new File(s);
                if (file.exists()) {
                    properties.putAll(extract(file));
                    break;
                }
            }
        } else {
            int separtorAt = value.indexOf(KEY_VALUE_SEPARATOR);
            if (separtorAt < 0) {
                properties.put(value, DEFAULT_VALUE);
            } else {
                properties.put(value.substring(0, separtorAt), value.substring(separtorAt + 1));
            }
        }
    }

    /**
     * Extracts properties in the target file.
     * @param file the target file (may not exist)
     * @return the extracted properties
     * @throws LaunchConfigurationException if the option is invalid
     */
    protected abstract Map<String, String> extract(File file) throws LaunchConfigurationException;

    @Override
    public Map<String, String> resolve() throws LaunchConfigurationException {
        return properties;
    }
}
