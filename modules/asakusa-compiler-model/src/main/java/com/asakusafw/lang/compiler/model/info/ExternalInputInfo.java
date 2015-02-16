package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.ValueDescription;

/**
 * Structural information of external inputs.
 */
public interface ExternalInputInfo extends ExternalPortInfo {

    /**
     * Returns the estimated data size.
     * @return the estimated data size
     */
    DataSize getDataSize();

    /**
     * Represents a kind of estimated input data size.
     */
    public enum DataSize {

        /**
         * unknown or not estimated.
         */
        UNKNOWN,

        /**
         * tiny data (~10MB)
         */
        TINY,

        /**
         * small data (~200MB)
         */
        SMALL,

        /**
         * large data (200MB~)
         */
        LARGE,
    }

    /**
     * A basic implementation of {@link ExternalInputInfo}.
     * Clients can inherit this class.
     */
    public static class Basic implements ExternalInputInfo {

        private final ClassDescription descriptionClass;

        private final ClassDescription dataModelClass;

        private final String moduleName;

        private final Map<String, ValueDescription> properties;

        private final DataSize dataSize;

        /**
         * Creates a new instance.
         * @param descriptionClass the original importer description class
         * @param dataModelClass the target data model class
         * @param moduleName the importer module name
         * @param dataSize the estimated data size
         * @param properties the importer properties
         */
        public Basic(
                ClassDescription descriptionClass,
                String moduleName,
                ClassDescription dataModelClass,
                DataSize dataSize,
                Map<String, ValueDescription> properties) {
            this.descriptionClass = descriptionClass;
            this.dataModelClass = dataModelClass;
            this.moduleName = moduleName;
            this.dataSize = dataSize;
            this.properties = Collections.unmodifiableMap(new LinkedHashMap<>(properties));
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(ExternalInputInfo info) {
            this(info.getDescriptionClass(), info.getModuleName(), info.getDataModelClass(),
                    info.getDataSize(), info.getProperties());
        }

        @Override
        public ClassDescription getDescriptionClass() {
            return descriptionClass;
        }

        @Override
        public ClassDescription getDataModelClass() {
            return dataModelClass;
        }

        @Override
        public String getModuleName() {
            return moduleName;
        }

        @Override
        public Map<String, ValueDescription> getProperties() {
            return properties;
        }

        @Override
        public DataSize getDataSize() {
            return dataSize;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "ExternalInput(module={0}, description={1})",
                    moduleName,
                    descriptionClass);
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + descriptionClass.hashCode();
            result = prime * result + moduleName.hashCode();
            result = prime * result + dataModelClass.hashCode();
            result = prime * result + dataSize.hashCode();
            result = prime * result + properties.hashCode();
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Basic other = (Basic) obj;
            if (!descriptionClass.equals(other.descriptionClass)) {
                return false;
            }
            if (!moduleName.equals(other.moduleName)) {
                return false;
            }
            if (!dataModelClass.equals(other.dataModelClass)) {
                return false;
            }
            if (dataSize != other.dataSize) {
                return false;
            }
            if (!properties.equals(other.properties)) {
                return false;
            }
            return true;
        }
    }
}
