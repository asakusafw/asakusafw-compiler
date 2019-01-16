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
package com.asakusafw.lang.compiler.extension.windgate;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import com.asakusafw.vocabulary.windgate.WindGateProcessDescription;
import com.asakusafw.windgate.core.DriverScript;

/**
 * Persistent WindGate description model.
 */
public class DescriptionModel implements Serializable {

    private static final long serialVersionUID = -836920664131767928L;

    private String profileName;

    private String resourceName;

    private HashMap<String, String> configuration;

    private Set<String> parameterNames;

    /**
     * for serializers.
     */
    protected DescriptionModel() {
        return;
    }

    /**
     * Creates a new instance.
     * @param description the original description
     */
    public DescriptionModel(WindGateProcessDescription description) {
        this.profileName = description.getProfileName();
        if (profileName == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "{1} must not be null: {0}",
                    description.getClass().getName(),
                    "getProfileName()")); //$NON-NLS-1$
        }
        if (profileName.isEmpty()) {
            throw new IllegalStateException(MessageFormat.format(
                    "{1} must not be empty string: {0}",
                    description.getClass().getName(),
                    "getProfileName()")); //$NON-NLS-1$
        }
        DriverScript script = description.getDriverScript();
        if (script == null) {
            throw new IllegalStateException(MessageFormat.format(
                    "Driver script is not defined: {0}",
                    description.getClass().getName()));
        }
        this.resourceName = script.getResourceName();
        this.configuration = new HashMap<>(script.getConfiguration());
        this.parameterNames = new HashSet<>(script.getParameterNames());
    }

    /**
     * Returns the profile name.
     * @return the profile name
     */
    public String getProfileName() {
        return profileName;
    }

    /**
     * Returns the driver script.
     * @return the driver script
     */
    public DriverScript getDriverScript() {
        return new DriverScript(resourceName, configuration, parameterNames);
    }
}
