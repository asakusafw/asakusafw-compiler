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
package com.asakusafw.bridge.launch;

import java.util.Set;

/**
 * An option for {@link LaunchConfiguration}.
 * @param <V> the option value type
 */
public interface LaunchOption<V> {

    /**
     * Returns the recognizable commands.
     * @return the recognizable commands
     */
    Set<String> getCommands();

    /**
     * Accepts a command and its value.
     * @param command the command token
     * @param value the command operand
     * @throws LaunchConfigurationException if failed to accept the command
     */
    void accept(String command, String value) throws LaunchConfigurationException;

    /**
     * Returns the option value.
     * @return the option value, or {@code null} if it is not set and this option is not mandatory
     * @throws LaunchConfigurationException if this option is incomplete
     */
    V resolve() throws LaunchConfigurationException;
}
