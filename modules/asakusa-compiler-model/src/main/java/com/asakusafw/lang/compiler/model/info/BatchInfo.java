package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.EnumSet;
import java.util.Set;
import java.util.regex.Pattern;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Structural information of batch.
 */
public interface BatchInfo {

    /**
     * Returns the batch ID.
     * @return the batch ID
     */
    String getBatchId();

    /**
     * Returns the original batch description class.
     * @return the batch description class
     */
    ClassDescription getDescriptionClass();

    /**
     * Returns a comment for this batch.
     * @return a comment, or {@code null} if there is no comments
     */
    String getComment();

    /**
     * Returns the batch parameters for this.
     * @return the batch parameters
     */
    Collection<BatchInfo.Parameter> getParameters();

    /**
     * Returns optional attributes for this.
     * @return attributes
     */
    Set<Attribute> getAttributes();

    /**
     * Attributes for Asakusa batches.
     */
    public enum Attribute {

        /**
         * Denies parameters other than defined.
         */
        STRICT_PARAMETERS,
    }

    /**
     * Parameter definition for Asakusa batches.
     */
    public static class Parameter {

        private final String key;

        private final String comment;

        private final boolean mandatory;

        private final Pattern pattern;

        /**
         * Creates a new instance.
         * @param key the batch parameter key
         * @param comment the parameter comment (nullable)
         * @param mandatory {@code true} if this parameter is mandatory, otherwise {@code false}
         * @param pattern the parameter value pattern (nullable)
         */
        public Parameter(String key, String comment, boolean mandatory, Pattern pattern) {
            this.key = key;
            this.comment = comment;
            this.mandatory = mandatory;
            this.pattern = pattern;
        }

        /**
         * Returns the batch parameter key.
         * @return the parameter key
         */
        public String getKey() {
            return key;
        }

        /**
         * Returns a comment for this parameter.
         * @return a comment, or {@code null} there is no comments for this
         */
        public String getComment() {
            return comment;
        }

        /**
         * Returns whether this parameter is mandatory or not.
         * @return {@code true} if this parameter is mandatory, otherwise {@code false}
         */
        public boolean isMandatory() {
            return mandatory;
        }

        /**
         * Returns the parameter value pattern in regular expression.
         * @return the parameter value pattern, or {@code null} if this parameter accepts any values
         */
        public Pattern getPattern() {
            return pattern;
        }

        @Override
        public String toString() {
            StringBuilder buf = new StringBuilder();
            buf.append("Parameter(");
            buf.append(key);
            if (mandatory) {
                buf.append("[*]"); //$NON-NLS-1$
            }
            if (comment != null) {
                buf.append(":");
                buf.append(comment);
            }
            buf.append(")");
            return buf.toString();
        }
    }

    /**
     * A basic implementation of {@link BatchInfo}.
     */
    public static class Basic implements BatchInfo {

        private final String batchId;

        private final ClassDescription descriptionClass;

        private final String comment;

        private final Collection<Parameter> parameters;

        private final Set<Attribute> attributes;

        /**
         * Creates a new instance.
         * @param batchId the batch ID
         * @param descriptionClass the original batch description class
         * @param comment a comment for this batch (nullable)
         * @param parameters parameters for this batch
         * @param attributes extra attributes for this batch
         */
        public Basic(
                String batchId,
                ClassDescription descriptionClass,
                String comment,
                Collection<Parameter> parameters,
                Collection<Attribute> attributes) {
            super();
            this.batchId = batchId;
            this.descriptionClass = descriptionClass;
            this.comment = comment;
            this.parameters = new ArrayList<>(parameters);
            this.attributes = EnumSet.noneOf(Attribute.class);
            this.attributes.addAll(attributes);
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(BatchInfo info) {
            this(info.getBatchId(), info.getDescriptionClass(),
                    info.getComment(), info.getParameters(), info.getAttributes());
        }

        @Override
        public String getBatchId() {
            return batchId;
        }

        @Override
        public ClassDescription getDescriptionClass() {
            return descriptionClass;
        }

        @Override
        public String getComment() {
            return comment;
        }

        @Override
        public Collection<Parameter> getParameters() {
            return parameters;
        }

        @Override
        public Set<Attribute> getAttributes() {
            return attributes;
        }
        @Override
        public String toString() {
            return MessageFormat.format(
                    "Batch(id={0}, description={1})", //$NON-NLS-1$
                    batchId,
                    descriptionClass.getName());
        }
    }
}
