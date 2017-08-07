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
package com.asakusafw.lang.compiler.extension.hive.info;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.info.hive.HiveIoAttribute;
import com.asakusafw.lang.compiler.api.reference.JobflowReference;
import com.asakusafw.lang.compiler.extension.hive.HiveSchemaAggregatorProcessor;
import com.asakusafw.lang.compiler.info.AttributeCollector;

/**
 * An implementation of {@link AttributeCollector} for Direct I/O files.
 * @since 0.5.0
 */
public class HiveIoAttributeCollector implements AttributeCollector {

    static final Logger LOG = LoggerFactory.getLogger(HiveIoAttributeCollector.class);

    @Override
    public void process(Context context, JobflowReference jobflow) {
        try {
            HiveIoAttribute attribute = HiveSchemaAggregatorProcessor.load(context::findResourceFile);
            context.putAttribute(attribute);
        } catch (LinkageError | Exception e) {
            LOG.debug("error occurred while loading Hive information", e);
        }
    }
}
