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
package com.asakusafw.lang.compiler.extension.directio;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.vocabulary.directio.DirectFileOutputDescription;

/**
 * Persistent direct file output description model.
 */
public class DirectFileOutputModel implements Serializable {

    private static final long serialVersionUID = -4546238057952201598L;

    private String basePath;

    private String resourcePattern;

    private String[] order;

    private String[] deletePatterns;

    private String formatClass;

    /**
     * Creates a new instance for serializers.
     */
    protected DirectFileOutputModel() {
        return;
    }

    /**
     * Creates a new instance.
     * @param description the original description
     */
    public DirectFileOutputModel(DirectFileOutputDescription description) {
        this.basePath = description.getBasePath();
        this.resourcePattern = description.getResourcePattern();
        this.order = array(description.getOrder());
        this.deletePatterns = array(description.getDeletePatterns());
        this.formatClass = description.getFormat().getName();
    }

    private String[] array(List<String> list) {
        return list.toArray(new String[list.size()]);
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
     * Returns the record order.
     * @return the record order
     */
    public List<String> getOrder() {
        return Arrays.asList(order);
    }

    /**
     * Returns the delete patterns.
     * @return the delete patterns
     */
    public List<String> getDeletePatterns() {
        return Arrays.asList(deletePatterns);
    }

    /**
     * Returns the format class.
     * @return the format class
     */
    public ClassDescription getFormatClass() {
        return new ClassDescription(formatClass);
    }
}
