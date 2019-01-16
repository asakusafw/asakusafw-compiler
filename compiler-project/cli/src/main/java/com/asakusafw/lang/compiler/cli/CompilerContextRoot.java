/**
 * Copyright 2011-2019 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.cli;

import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.ProjectRepository;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;

/**
 * Root {@link CompilerContext} which accepts batch specific settings.
 */
public class CompilerContextRoot {

    static final Logger LOG = LoggerFactory.getLogger(CompilerContextRoot.class);

    private static final char SCOPE_SEPARATOR = ':';

    private final CompilerContext root;

    /**
     * Creates a new instance.
     * @param root the root context
     */
    public CompilerContextRoot(CompilerContext root) {
        this.root = root;
    }

    /**
     * Returns the root context.
     * @return the root context
     */
    public CompilerContext getRoot() {
        return root;
    }

    /**
     * Returns {@link CompilerContext} for the target batch.
     * @param batch the target batch information
     * @return the scoped context
     */
    public CompilerContext getScopedContext(BatchInfo batch) {
        CompilerOptions scopedOptions = createScopedOptions(batch);
        ProjectRepository project = root.getProject();
        ToolRepository tools = root.getTools();
        FileContainerRepository temporary = root.getTemporaryOutputs();
        return new CompilerContext.Basic(scopedOptions, project, tools, temporary);
    }

    private CompilerOptions createScopedOptions(BatchInfo info) {
        CompilerOptions options = root.getOptions();
        Map<String, String> properties = new LinkedHashMap<>();
        Map<String, String> scoped = new LinkedHashMap<>();
        for (Map.Entry<String, String> entry : options.getRawProperties().entrySet()) {
            String rawKey = entry.getKey();
            String scope = getOptionKeyScope(rawKey);
            String key = getBareOptionKey(rawKey);
            String value = entry.getValue();
            if (scope == null) {
                properties.put(rawKey, value);
            } else if (scope.equals(info.getBatchId())) {
                LOG.debug("activate option: {}={}", key, value); //$NON-NLS-1$
                scoped.put(key, value);
            }
        }
        properties.putAll(scoped);
        return new CompilerOptions(options.getBuildId(), options.getRuntimeWorkingDirectory(), properties);
    }

    private String getOptionKeyScope(String rawKey) {
        int index = rawKey.indexOf(SCOPE_SEPARATOR);
        if (index < 0) {
            return null;
        }
        return rawKey.substring(0, index);
    }

    private String getBareOptionKey(String rawKey) {
        int index = rawKey.indexOf(':');
        if (index < 0) {
            return rawKey;
        }
        return rawKey.substring(index + 1);
    }
}
