package com.asakusafw.lang.compiler.model.info;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.description.ClassDescription;

/**
 * Structural information of jobflows.
 */
public interface JobflowInfo extends DescriptionInfo {

    /**
     * Returns the flow ID.
     * @return the flow ID
     */
    String getFlowId();

    /**
     * Returns the original flow description class.
     * @return the flow description class
     */
    @Override
    ClassDescription getDescriptionClass();

    /**
     * A basic implementation of {@link JobflowInfo}.
     * Clients can inherit this class.
     */
    public static class Basic implements JobflowInfo {

        private final String flowId;

        private final ClassDescription descriptionClass;

        /**
         * Creates a new instance.
         * @param flowId the flow ID
         * @param descriptionClass the original flow description class name
         */
        public Basic(String flowId, ClassDescription descriptionClass) {
            this.flowId = flowId;
            this.descriptionClass = descriptionClass;
        }

        /**
         * Creates a new instance.
         * @param info the original information
         */
        public Basic(JobflowInfo info) {
            this(info.getFlowId(), info.getDescriptionClass());
        }

        @Override
        public String getFlowId() {
            return flowId;
        }

        @Override
        public ClassDescription getDescriptionClass() {
            return descriptionClass;
        }

        @Override
        public String toString() {
            return MessageFormat.format(
                    "Jobflow(id={0}, description={1})", //$NON-NLS-1$
                    flowId,
                    descriptionClass.getName());
        }
    }
}
