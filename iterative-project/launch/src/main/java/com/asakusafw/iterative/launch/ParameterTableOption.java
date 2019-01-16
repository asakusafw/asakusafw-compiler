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
package com.asakusafw.iterative.launch;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.bridge.launch.AbstractFileOption;
import com.asakusafw.bridge.launch.LaunchConfigurationException;
import com.asakusafw.bridge.launch.LaunchOption;
import com.asakusafw.iterative.common.IterativeExtensions;
import com.asakusafw.iterative.common.ParameterTable;

/**
 * A {@link LaunchOption} about {@link ParameterTable}s.
 * @since 0.3.0
 */
public class ParameterTableOption extends AbstractFileOption<ParameterTable> {

    static final Logger LOG = LoggerFactory.getLogger(ParameterTableOption.class);

    /**
     * The command name.
     */
    public static final String COMMAND = "--parameter-table"; //$NON-NLS-1$

    private ParameterTable result;

    @Override
    public Set<String> getCommands() {
        return Collections.singleton(COMMAND);
    }

    @Override
    protected void accept(String command, File value) throws LaunchConfigurationException {
        if (result != null) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "duplicate launch option: {0}",
                    command));
        }
        LOG.debug("extracting parameter table: {}", value); //$NON-NLS-1$
        try (InputStream input = new FileInputStream(value)) {
            result = IterativeExtensions.load(input);
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
}
