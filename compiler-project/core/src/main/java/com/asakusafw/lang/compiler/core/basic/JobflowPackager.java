/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.basic;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.function.Predicate;
import java.util.jar.JarFile;
import java.util.jar.JarOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Location;
import com.asakusafw.lang.compiler.common.ResourceContainer;
import com.asakusafw.lang.compiler.packaging.ResourceAssembler;
import com.asakusafw.lang.compiler.packaging.ResourceRepository;
import com.asakusafw.lang.compiler.packaging.ResourceSink;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;
import com.asakusafw.lang.compiler.packaging.ZipSink;

/**
 * Creates jobflow packages.
 */
public class JobflowPackager {

    static final Logger LOG = LoggerFactory.getLogger(JobflowPackager.class);

    /**
     * The location of manifest file in package.
     */
    static final Location MANIFEST_FILE = Location.of(JarFile.MANIFEST_NAME);

    /**
     * The jobflow element location of package meta info.
     */
    static final Location PACKAGE_META_INFO = MANIFEST_FILE.getParent();

    /**
     * The jobflow element location of framework info.
     */
    static final Location FRAMEWORK_INFO = PACKAGE_META_INFO.append("asakusa"); //$NON-NLS-1$

    /**
     * The jobflow element location of fragment marker file in each class library.
     */
    public static final Location FRAGMENT_MARKER = FRAMEWORK_INFO.append("fragment"); //$NON-NLS-1$

    /**
     * The batch element location of jobflow libraries.
     */
    static final Location JOBFLOW_LIBRARIES = Location.of("lib"); //$NON-NLS-1$

    /**
     * The file name pattern of jobflow libraries.
     */
    static final String PATTERN_JOBFLOW_LIBRARY = "jobflow-{0}.jar"; //$NON-NLS-1$

    private static final Predicate<Location> EMBEDDED_CONTENT_ACCEPTOR = location -> {
        if (location.equals(MANIFEST_FILE)) {
            return false;
        }
        if (FRAMEWORK_INFO.isPrefixOf(location)) {
            return false;
        }
        return true;
    };

    /**
     * Creates a jobflow package into batch output container.
     * @param flowId the target flow ID
     * @param batchOutput the target batch output container
     * @param jobflowOutput the source jobflow output container
     * @param jobflowEmbedded the jobflow embedded contents
     * @throws IOException if failed to create a jobflow package
     */
    public void process(
            String flowId,
            ResourceContainer batchOutput,
            ResourceRepository jobflowOutput,
            Collection<? extends ResourceRepository> jobflowEmbedded) throws IOException {
        LOG.debug("building jobflow package: {}->{}", flowId, batchOutput); //$NON-NLS-1$
        ResourceRepository result = assemble(jobflowOutput, jobflowEmbedded);
        Location location = getLibraryLocation(flowId);
        try (ResourceSink sink = new ZipSink(new JarOutputStream(batchOutput.addResource(location)))) {
            ResourceUtil.copy(result, sink);
        }
    }


    private ResourceRepository assemble(ResourceRepository output, Collection<? extends ResourceRepository> embedded) {
        ResourceAssembler assembler = new ResourceAssembler();
        assembler.addRepository(output);
        for (ResourceRepository repository : embedded) {
            assembler.addRepository(repository, EMBEDDED_CONTENT_ACCEPTOR);
        }
        ResourceRepository result = assembler.build();
        return result;
    }

    /**
     * Returns the jobflow library location.
     * @param flowId the target flow ID
     * @return the library location (relative from the batch package root)
     */
    public static Location getLibraryLocation(String flowId) {
        String name = MessageFormat.format(PATTERN_JOBFLOW_LIBRARY, flowId);
        return JOBFLOW_LIBRARIES.append(name);
    }
}
