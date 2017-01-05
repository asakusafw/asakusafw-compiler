/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.extension.directio;

import java.io.Serializable;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.vocabulary.directio.DirectFileInputDescription;

/**
 * Persistent direct file input description model.
 */
public class DirectFileInputModel implements Serializable {

    private static final long serialVersionUID = -4304774107461133202L;

    private String basePath;

    private String resourcePattern;

    private String formatClass;

    private String filterClass;

    private boolean optional;

    /**
     * Creates a new instance for serializers.
     */
    protected DirectFileInputModel() {
        return;
    }

    /**
     * Creates a new instance.
     * @param description the original description
     */
    public DirectFileInputModel(DirectFileInputDescription description) {
        this.basePath = description.getBasePath();
        this.resourcePattern = description.getResourcePattern();
        this.formatClass = toName(description.getFormat());
        this.filterClass = toName(description.getFilter());
        this.optional = description.isOptional();
    }

    /**
     * Returns the base path.
     * @return the base path
     */
    public String getBasePath() {
        return basePath;
    }

    /**
     * Returns the resource pattern.
     * @return the resource pattern
     */
    public String getResourcePattern() {
        return resourcePattern;
    }

    /**
     * Returns the format class.
     * @return the format class
     */
    public ClassDescription getFormatClass() {
        return toClass(formatClass);
    }

    /**
     * Returns the filter class.
     * @return the filter class, or {@code null} if it is not defined
     */
    public ClassDescription getFilterClass() {
        return toClass(filterClass);
    }

    /**
     * Returns whether the target input is optional or not.
     * @return {@code true} if the target input is optional, otherwise {@code false}
     */
    public boolean isOptional() {
        return optional;
    }

    private static String toName(Class<?> aClass) {
        if (aClass == null) {
            return null;
        }
        return aClass.getName();
    }

    private static ClassDescription toClass(String className) {
        if (className == null) {
            return null;
        }
        return new ClassDescription(className);
    }
}
