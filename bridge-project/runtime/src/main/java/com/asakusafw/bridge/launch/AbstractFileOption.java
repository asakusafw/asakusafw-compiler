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
package com.asakusafw.bridge.launch;

import java.io.File;
import java.text.MessageFormat;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract {@link LaunchOption} which accepts files.
 * @param <V> the option value type
 * @since 0.3.0
 */
public abstract class AbstractFileOption<V> implements LaunchOption<V> {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractFileOption.class);

    /**
     * The prefix of file path.
     */
    public static final char FILE_PREFIX = '@';

    /**
     * The selective file separator.
     */
    public static final char FILE_SEPARATOR = '|';

    @Override
    public void accept(String command, String value) throws LaunchConfigurationException {
        String fileList = value;
        if (fileList.isEmpty() == false && fileList.charAt(0) == FILE_PREFIX) {
            fileList = fileList.substring(1);
        }
        if (fileList.isEmpty()) {
            return;
        }
        for (String s : fileList.split(Pattern.quote(String.valueOf(FILE_SEPARATOR)))) {
            if (s.isEmpty()) {
                continue;
            }
            File file = new File(s);
            if (LOG.isDebugEnabled()) {
                LOG.debug("parsing {}: {}", command, s); //$NON-NLS-1$
            }
            if (file.exists()) {
                LOG.debug("resolving file {}: {}", command, s); //$NON-NLS-1$
                accept(command, file);
                return;
            } else {
                LOG.debug("missing file {}: {}", command, s); //$NON-NLS-1$
            }
        }
        throw new LaunchConfigurationException(MessageFormat.format(
                "no available files ({0}): {1}",
                command,
                value));
    }

    /**
     * Accepts a command and its file.
     * @param command the command token
     * @param value the command operand
     * @throws LaunchConfigurationException if failed to accept the command
     */
    protected abstract void accept(String command, File value) throws LaunchConfigurationException;
}
