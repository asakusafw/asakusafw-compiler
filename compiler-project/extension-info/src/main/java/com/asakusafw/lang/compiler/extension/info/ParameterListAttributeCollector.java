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
package com.asakusafw.lang.compiler.extension.info;

import java.util.Collection;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.asakusafw.info.ParameterInfo;
import com.asakusafw.info.ParameterListAttribute;
import com.asakusafw.lang.compiler.info.AttributeCollector;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.parameter.ImplicitParameterList;

/**
 * Collects {@link ParameterListAttribute}.
 * @since 0.4.1
 * @version 0.5.0
 */
public class ParameterListAttributeCollector implements AttributeCollector {

    @Override
    public void process(Context context, Batch batch) {
        Collection<BatchInfo.Parameter> parameters = Optional
                .ofNullable(context.getExtension(ImplicitParameterList.class))
                .map(it -> it.merge(batch.getParameters()))
                .orElseGet(batch::getParameters);

        context.putAttribute(new ParameterListAttribute(
                parameters.stream()
                    .map(p -> new ParameterInfo(
                            p.getKey(),
                            p.getComment(),
                            p.isMandatory(),
                            Optional.ofNullable(p.getPattern())
                                .map(Pattern::pattern)
                                .orElse(null)))
                    .collect(Collectors.toList()),
                batch.getAttributes().contains(BatchInfo.Attribute.STRICT_PARAMETERS)));
    }
}
