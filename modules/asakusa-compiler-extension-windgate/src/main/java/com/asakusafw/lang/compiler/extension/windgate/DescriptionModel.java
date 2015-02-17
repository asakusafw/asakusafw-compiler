package com.asakusafw.lang.compiler.extension.windgate;

import java.io.Serializable;
import java.text.MessageFormat;
import java.util.HashMap;

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
        return new DriverScript(resourceName, configuration);
    }
}
