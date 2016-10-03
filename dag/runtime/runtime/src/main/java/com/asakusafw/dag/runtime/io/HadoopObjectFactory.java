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
package com.asakusafw.dag.runtime.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;

import com.asakusafw.dag.api.common.ObjectFactory;
import com.asakusafw.lang.utils.common.Arguments;

/**
 * An {@link ObjectFactory} implementation for Hadoop-related objects.
 * @since 0.4.0
 */
public class HadoopObjectFactory implements ObjectFactory {

    private final Configuration configuration;

    /**
     * Creates a new instance.
     * @param configuration the current Hadoop configuration
     */
    public HadoopObjectFactory(Configuration configuration) {
        Arguments.requireNonNull(configuration);
        this.configuration = configuration;
    }

    @Override
    public <T> T newInstance(Class<T> aClass) {
        Arguments.requireNonNull(aClass);
        return ReflectionUtils.newInstance(aClass, configuration);
    }
}
