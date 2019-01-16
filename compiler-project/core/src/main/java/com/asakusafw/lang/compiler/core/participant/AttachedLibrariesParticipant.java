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
package com.asakusafw.lang.compiler.core.participant;

import java.io.IOException;
import java.io.InputStream;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.reference.BatchReference;
import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.util.StringUtil;
import com.asakusafw.lang.compiler.core.BatchCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.model.graph.Batch;
import com.asakusafw.lang.compiler.packaging.FileContainer;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;

/**
 * Deploys project attached libraries into batch packages.
 */
public class AttachedLibrariesParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(AttachedLibrariesParticipant.class);

    /**
     * The output directory path in the final artifact.
     */
    static final Location LOCATION = Location.of("usr/lib"); //$NON-NLS-1$

    @Override
    public void afterBatch(Context context, Batch batch, BatchReference reference) {
        LOG.debug("attaching project libraries into batch: {}", batch.getBatchId()); //$NON-NLS-1$
        FileContainer output = context.getOutput();
        Locator locator = new Locator();
        for (ResourceRepository repository : context.getProject().getAttachedLibraries()) {
            try (ResourceRepository.Cursor cursor = repository.createCursor()) {
                while (cursor.next()) {
                    Location source = cursor.getLocation();
                    Location destination = locator.getLocation(source);
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("deploying attached library: {}@{} -> {}@{}", new Object[] { //$NON-NLS-1$
                                source,
                                repository,
                                destination,
                                batch.getBatchId(),
                        });
                    }
                    try (InputStream input = cursor.openResource()) {
                        output.addResource(destination, input);
                    }
                }
            } catch (IOException e) {
                throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                        "failed to deploy attached libraries: {0}",
                        repository));
            }
        }
    }

    private static final class Locator {

        private final Set<String> saw = new HashSet<>();

        Locator() {
            return;
        }

        Location getLocation(Location source) {
            String name = computeUniqueName(source.getName());
            assert saw.contains(name) == false;
            saw.add(name);
            return LOCATION.append(name);
        }

        private String computeUniqueName(String name) {
            if (saw.contains(name) == false) {
                return name;
            }
            int dotAt = name.lastIndexOf('.');
            String prefix;
            String suffix;
            if (dotAt <= 0) { // may be a dot file
                prefix = name;
                suffix = StringUtil.EMPTY;
            } else {
                prefix = name.substring(0, dotAt);
                suffix = name.substring(dotAt);
            }
            for (int i = 0; i <= 9999; i++) {
                String rename = String.format("%s_%d%s", prefix, i, suffix); //$NON-NLS-1$
                if (saw.contains(rename)) {
                    continue;
                }
                return rename;
            }
            return String.format("%s_%s%s", prefix, UUID.randomUUID(), suffix); //$NON-NLS-1$
        }
    }
}
