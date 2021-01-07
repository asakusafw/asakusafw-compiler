/**
 * Copyright 2011-2021 Asakusa Framework Team.
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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Set;

/**
 * A {@link LaunchOption} about stage client class.
 */
public class StageClientOption implements LaunchOption<Class<?>> {

    /**
     * The command name for stage client.
     */
    public static final String COMMAND = "--client"; //$NON-NLS-1$

    private final ClassLoader classLoader;

    private Class<?> result;

    /**
     * Creates a new instance.
     * @param classLoader the class loader to load client class
     */
    public StageClientOption(ClassLoader classLoader) {
        this.classLoader = classLoader;
    }

    @Override
    public Set<String> getCommands() {
        return Collections.singleton(COMMAND);
    }

    @Override
    public void accept(String command, String value) throws LaunchConfigurationException {
        if (result != null) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "duplicate stage client: {0} {1}",
                    command, value));
        }
        try {
            result = classLoader.loadClass(value);
        } catch (ClassNotFoundException e) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "failed to load stage client: {0} {1}",
                    command, value), e);
        }
    }

    @Override
    public Class<?> resolve() throws LaunchConfigurationException {
        if (result == null) {
            throw new LaunchConfigurationException(MessageFormat.format(
                    "missing stage client: {0} {1}",
                    COMMAND));
        }
        return result;
    }
}
