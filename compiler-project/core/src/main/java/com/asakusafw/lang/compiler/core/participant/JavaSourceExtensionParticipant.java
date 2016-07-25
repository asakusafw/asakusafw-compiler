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
package com.asakusafw.lang.compiler.core.participant;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.JobflowCompiler.Context;
import com.asakusafw.lang.compiler.core.basic.AbstractCompilerParticipant;
import com.asakusafw.lang.compiler.javac.BasicJavaCompilerSupport;
import com.asakusafw.lang.compiler.javac.JavaCompilerUtil;
import com.asakusafw.lang.compiler.javac.JavaSourceExtension;
import com.asakusafw.lang.compiler.model.graph.Jobflow;
import com.asakusafw.lang.compiler.model.info.BatchInfo;
import com.asakusafw.lang.compiler.model.info.JobflowInfo;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * A compiler participant for enabling {@link JavaSourceExtension}.
 *
 * <h3> compiler options </h3>
 * <ul>
 * <li> {@code javac.version} (optional)
 *   <ul>
 *   <li> the compiler compliance version (a.k.a. {@code -source, -target}) </li>
 *   <li> <em>default value</em>: {@code "1.8"} </li>
 *   </ul>
 * </li>
 * <li> {@code javac.bootclasspath} (optional)
 *   <ul>
 *   <li> the bootstrap class path  (a.k.a. {@code -bootclasspath}) </li>
 *   <li> <em>default value</em>: (current Java VM's boot classpath) </li>
 *   </ul>
 * </li>
 * </ul>
 */
public class JavaSourceExtensionParticipant extends AbstractCompilerParticipant {

    static final Logger LOG = LoggerFactory.getLogger(JavaSourceExtensionParticipant.class);

    private static final String KEY_PREFIX = "javac."; //$NON-NLS-1$

    /**
     * The compiler option key of Java compiler compliance version (a.k.a. {@code -source, -target}).
     */
    public static final String KEY_VERSION = KEY_PREFIX + "version"; //$NON-NLS-1$

    /**
     * The compiler option key of bootstrap class path (a.k.a. {@code -bootclasspath}).
     */
    public static final String KEY_BOOT_CLASSPATH = KEY_PREFIX + "bootclasspath"; //$NON-NLS-1$

    @Override
    public void beforeJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        LOG.debug("enabling {}", JavaSourceExtension.class.getName()); //$NON-NLS-1$
        List<File> classPath = JavaCompilerUtil.getLibraries(context.getProject().getClassLoader());
        File sourcePath = createTemporaryOutput(context, jobflow);
        BasicJavaCompilerSupport extension = new BasicJavaCompilerSupport(
                sourcePath,
                classPath,
                context.getOutput().getBasePath());
        configure(context, extension);
        context.registerExtension(JavaSourceExtension.class, extension);
    }

    private void configure(Context context, BasicJavaCompilerSupport extension) {
        String version = context.getOptions().get(KEY_VERSION, null);
        if (version != null && version.isEmpty() == false) {
            LOG.debug("detect option: {} = {}", KEY_VERSION, version); //$NON-NLS-1$
            extension.withCompliantVersion(version);
        }
        String bootclasspath = context.getOptions().get(KEY_BOOT_CLASSPATH, null);
        if (bootclasspath != null) {
            List<File> files = new ArrayList<>();
            for (String s : bootclasspath.split(Pattern.quote(File.pathSeparator))) {
                if (s.isEmpty() == false) {
                    files.add(new File(s));
                }
            }
            if (files.isEmpty() == false) {
                LOG.debug("detect option: {} = {}", KEY_BOOT_CLASSPATH, files); //$NON-NLS-1$
                extension.withBootClassPath(files);
            }
        }
    }

    @Override
    public void afterJobflow(Context context, BatchInfo batch, Jobflow jobflow) {
        JavaSourceExtension extension = context.getExtension(JavaSourceExtension.class);
        if ((extension instanceof BasicJavaCompilerSupport) == false) {
            return;
        }
        context.registerExtension(JavaSourceExtension.class, null);

        BasicJavaCompilerSupport javac = (BasicJavaCompilerSupport) extension;
        javac.process();
        ResourceUtil.delete(javac.getSourcePath());
    }

    private File createTemporaryOutput(Context context, JobflowInfo jobflow) {
        String prefix = String.format("javac-%s", jobflow.getFlowId()); //$NON-NLS-1$
        try {
            return context.getTemporaryOutputs().newContainer(prefix).getBasePath();
        } catch (IOException e) {
            throw new DiagnosticException(Diagnostic.Level.ERROR, MessageFormat.format(
                    "failed to create temporary output: {0}",
                    context.getTemporaryOutputs().getRoot()));
        }
    }
}
