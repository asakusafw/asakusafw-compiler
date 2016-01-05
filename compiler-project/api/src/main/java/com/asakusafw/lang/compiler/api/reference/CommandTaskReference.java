/**
 * Copyright 2011-2016 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.api.reference;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.common.BasicAttributeContainer;
import com.asakusafw.lang.compiler.common.Location;

/**
 * A symbol of task with any command.
 * @since 0.1.0
 * @version 0.3.0
 */
public class CommandTaskReference extends BasicAttributeContainer implements TaskReference {

    private static final Pattern PATTERN_MODULE_NAME = Pattern.compile("[a-z0-9\\-]+"); //$NON-NLS-1$

    private final String moduleName;

    private final String profileName;

    private final Location command;

    private final List<CommandToken> arguments;

    private final Set<String> extensions;

    private final List<TaskReference> blockerTasks;

    /**
     * Creates a new instance.
     * @param moduleName the target module name only consists of lower-letters and digits
     * @param profileName the profile name where the command is running
     * @param command the command path (relative from {@code ASAKUSA_HOME})
     * @param arguments the command arguments
     * @param blockerTasks the blocker tasks
     */
    public CommandTaskReference(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            List<? extends TaskReference> blockerTasks) {
        this(moduleName, profileName, command, arguments, Collections.<String>emptySet(), blockerTasks);
    }

    /**
     * Creates a new instance.
     * @param moduleName the target module name only consists of lower-letters and digits
     * @param profileName the profile name where the command is running
     * @param command the command path (relative from {@code ASAKUSA_HOME})
     * @param arguments the command arguments
     * @param extensions the acceptable extension names
     * @param blockerTasks the blocker tasks
     * @since 0.3.0
     */
    public CommandTaskReference(
            String moduleName,
            String profileName,
            Location command,
            List<? extends CommandToken> arguments,
            Collection<String> extensions,
            List<? extends TaskReference> blockerTasks) {
        if (PATTERN_MODULE_NAME.matcher(moduleName).matches() == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "module name must consist of lower-case alphabets and digits: \"{0}\"", //$NON-NLS-1$
                    moduleName));
        }
        this.moduleName = moduleName;
        this.profileName = profileName;
        this.command = command;
        this.arguments = Collections.unmodifiableList(new ArrayList<>(arguments));
        this.extensions = Collections.unmodifiableSet(new LinkedHashSet<>(extensions));
        this.blockerTasks = Collections.unmodifiableList(new ArrayList<>(blockerTasks));
    }

    @Override
    public String getModuleName() {
        return moduleName;
    }

    @Override
    public Set<String> getExtensions() {
        return extensions;
    }

    @Override
    public List<TaskReference> getBlockers() {
        return blockerTasks;
    }

    /**
     * Returns the profile name where the command is running.
     * @return the profile name
     */
    public String getProfileName() {
        return profileName;
    }

    /**
     * Returns the target command path.
     * @return the target command path (relative from {@code ASAKUSA_HOME})
     */
    public Location getCommand() {
        return command;
    }

    /**
     * Returns the command arguments.
     * @return the command arguments
     */
    public List<CommandToken> getArguments() {
        return arguments;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "CommandTask({0}@{1}:{2})", //$NON-NLS-1$
                moduleName,
                profileName,
                command);
    }
}
