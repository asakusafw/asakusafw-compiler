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
package com.asakusafw.lang.compiler.tester;

import java.io.File;
import java.io.IOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.core.CompilerContext;
import com.asakusafw.lang.compiler.core.ProjectRepository;
import com.asakusafw.lang.compiler.core.ToolRepository;
import com.asakusafw.lang.compiler.packaging.FileContainerRepository;
import com.asakusafw.lang.compiler.packaging.FilePackageBuilder;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Profiles for {@link CompilerTester}.
 *
 * <h3 id="required"> required settings </h3>
 * <p>
 * The following properties are required for building a {@link CompilerTester}:
 * </p>
 * <ul>
 * <li> embedded resources (e.g. data model or operator classes) - {@link #forProjectRepository()}  </li>
 * <li> attached libraries (e.g. application dependent libraries) - {@link #forProjectRepository()}  </li>
 * <li> data model processors - {@link #forToolRepository()} </li>
 * <li> external port processors - {@link #forToolRepository()} </li>
 * <li> jobflow processors - {@link #forToolRepository()} </li>
 * <li> batch processors - {@link #forToolRepository()} </li>
 * </ul>
 *
 * <h3 id="installation"> framework installations </h3>
 * <p>
 * Clients can choice from two Asakusa framework installation types:
 * </p>
 * <ul>
 * <li>
 *   <em> deployed installations </em>
 *   <ul>
 *   <li> uses the already deployed framework installation on the system </li>
 *   <li> {@link #withFrameworkInstallation(File)} </li>
 *   </ul>
 * </li>
 * <li>
 *   <em> volatile installations </em>
 *   <ul>
 *   <li> creates a new framework installation, and dispose after the current session was finished </li>
 *   <li> {@link #forFrameworkInstallation()} </li>
 *   </ul>
 * </li>
 * </ul>
 * @since 0.1.0
 * @version 0.3.0
 */
public final class CompilerProfile {

    static final Logger LOG = LoggerFactory.getLogger(CompilerProfile.class);

    private final ClassLoader baseClassLoader;

    private final CompilerOptions.Builder compilerOptions;

    private final ProjectRepository.Builder projectRepository;

    private final ToolRepository.Builder toolRepository;

    private final FilePackageBuilder frameworkInstallation;

    private final Map<String, String> environmentVariables = new LinkedHashMap<>();

    private File explicitFrameworkHome;

    private boolean frameworkHomeEnabled = true;

    /**
     * Creates a new instance.
     * @param classLoader the class loader, which includes application and compiler tool classes
     */
    public CompilerProfile(ClassLoader classLoader) {
        this.baseClassLoader = classLoader;
        this.compilerOptions = CompilerOptions.builder();
        this.projectRepository = ProjectRepository.builder(classLoader);
        this.toolRepository = ToolRepository.builder(classLoader);
        this.frameworkInstallation = new FilePackageBuilder();
    }

    /**
     * Returns the class loader.
     * @return the class loader, which includes application and compiler tool classes
     */
    public ClassLoader getClassLoader() {
        return baseClassLoader;
    }

    /**
     * Applies an edit.
     * @param edit the edit to apply
     * @return this
     */
    public CompilerProfile apply(Edit edit) {
        try {
            edit.perform(this);
            return this;
        } catch (IOException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "exception occurred while performing edit: {0}",
                    edit), e);
        }
    }

    /**
     * Returns a builder for {@link CompilerOptions}.
     * @return the builder
     */
    public CompilerOptions.Builder forCompilerOptions() {
        return compilerOptions;
    }

    /**
     * Returns a builder for {@link ProjectRepository}.
     * @return the builder
     */
    public ProjectRepository.Builder forProjectRepository() {
        return projectRepository;
    }

    /**
     * Returns a builder for {@link ToolRepository}.
     * @return the builder
     */
    public ToolRepository.Builder forToolRepository() {
        return toolRepository;
    }

    /**
     * Returns a builder for preparing <em>volatile Asakusa framework installations</em>.
     * Note that, if {@link #withFrameworkInstallation(File) explicit framework install path} is set,
     * this operation will break down the existing framework installation.
     * @return the builder
     */
    public FilePackageBuilder forFrameworkInstallation() {
        return frameworkInstallation;
    }

    /**
     * Adds environment variables.
     * @param map the environment variable map
     * @return this
     */
    public CompilerProfile withEnvironmentVariables(Map<String, String> map) {
        for (Map.Entry<String, String> entry : map.entrySet()) {
            String name = entry.getKey();
            String value = entry.getValue();
            if (value == null) {
                environmentVariables.remove(name);
            } else {
                environmentVariables.put(name, value);
            }
        }
        return this;
    }

    /**
     * Declares to use the specified Asakusa framework installation, instead of <em>volatile installation</em>.
     * If the {@code path} is {@code null}, the framework installation is disabled (an empty installation will be
     * provided).
     * @param path the installation path, or {@code null} to disable framework installation
     * @return this
     * @see <a href="#installation"> framework installations </a>
     */
    public CompilerProfile withFrameworkInstallation(File path) {
        this.frameworkHomeEnabled = path != null;
        this.explicitFrameworkHome = path;
        return this;
    }

    /**
     * Builds and returns a {@link CompilerTester} using this profile.
     * @return the built object
     * @throws IOException if failed to prepare the object by I/O error
     */
    public CompilerTester build() throws IOException {
        List<File> temporaryFiles = new ArrayList<>();
        TesterContext testerContext = buildTesterContext(temporaryFiles);
        boolean success = false;
        try {
            buildOnTheFlyInstallation(testerContext);
            CompilerContext compilerContext = buildCompilerContext(temporaryFiles);
            CompilerTester result = new CompilerTester(testerContext, compilerContext);
            success = true;
            // registers temporary files
            for (File file : temporaryFiles) {
                testerContext.addTemporaryFile(file);
            }
            temporaryFiles.clear();
            return result;
        } finally {
            if (success == false) {
                for (File file : temporaryFiles) {
                    if (file.exists() && ResourceUtil.delete(file) == false) {
                        LOG.warn(MessageFormat.format(
                                "failed to delete temporary file: {0}",
                                file));
                    }
                }
            }
        }
    }

    private TesterContext buildTesterContext(List<File> temporaryFiles) throws IOException {
        Map<String, String> env = new LinkedHashMap<>(environmentVariables);
        TesterContext result = new TesterContext(baseClassLoader, env);
        if (explicitFrameworkHome != null) {
            env.put(TesterContext.ENV_FRAMEWORK_PATH, explicitFrameworkHome.getAbsolutePath());
        } else {
            File home = newTemporaryFolder(temporaryFiles);
            env.put(TesterContext.ENV_FRAMEWORK_PATH, home.getAbsolutePath());
        }
        File batchapps = newTemporaryFolder(temporaryFiles);
        env.put(TesterContext.ENV_BATCHAPPS_PATH, batchapps.getAbsolutePath());
        return result;
    }

    private CompilerContext buildCompilerContext(List<File> temporaryFiles) throws IOException {
        return new CompilerContext.Basic(
                compilerOptions.build(),
                projectRepository.build(),
                toolRepository.build(),
                new FileContainerRepository(newTemporaryFolder(temporaryFiles)));
    }

    private File newTemporaryFolder(List<File> temporaryFiles) throws IOException {
        File temporary = File.createTempFile("asakusa", ".tmp"); //$NON-NLS-1$ //$NON-NLS-2$
        if (temporary.delete() == false || temporary.mkdirs() == false) {
            throw new IOException("failed to create a compiler temporary working directory");
        }
        temporaryFiles.add(temporary);
        return temporary;
    }

    private void buildOnTheFlyInstallation(TesterContext context) throws IOException {
        if (frameworkHomeEnabled == false) {
            LOG.debug("Asakusa framework installation is disabled");
            if (frameworkInstallation.isEmpty() == false) {
                LOG.warn("ignored some framework installation files (installation is disabled)");
            }
            return;
        }
        File home = context.getFrameworkHome();
        if (frameworkInstallation.isEmpty()) {
            if (explicitFrameworkHome == null) {
                LOG.warn(MessageFormat.format(
                        "volatile Asakusa framework will be empty: {0}",
                        home));
            }
        }
        frameworkInstallation.build(home);
    }

    /**
     * A callback object for {@link CompilerProfile}.
     */
    @FunctionalInterface
    public interface Edit {

        /**
         * Edits the target {@link CompilerProfile}.
         * @param profile the target profile
         * @throws IOException if I/O error occurred while editing the target
         */
        void perform(CompilerProfile profile) throws IOException;
    }
}
