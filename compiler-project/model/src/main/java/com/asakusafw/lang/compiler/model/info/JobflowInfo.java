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
                    descriptionClass.getClassName());
        }
    }
}
