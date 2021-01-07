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
package com.asakusafw.lang.compiler.javac;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import javax.tools.DiagnosticListener;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.StandardLocation;

import com.asakusafw.lang.compiler.common.Location;

/**
 * A basic implementation of {@link JavaCompilerSupport}.
 */
public class BasicJavaCompilerSupport extends AbstractJavaCompilerSupport {

    private final File sourcePath;

    private final List<File> classPath;

    private final File destinationPath;

    private volatile List<File> bootClassPath;

    private volatile String compliantVersion;

    /**
     * Creates a new instance.
     * @param sourcePath the source path
     * @param classPath the class path
     * @param destinationPath the compile output path
     */
    public BasicJavaCompilerSupport(File sourcePath, List<File> classPath, File destinationPath) {
        this.sourcePath = sourcePath;
        this.classPath = Collections.unmodifiableList(new ArrayList<>(classPath));
        this.destinationPath = destinationPath;
    }

    /**
     * Sets the boot class path.
     * @param newValue the value
     * @return this
     */
    public BasicJavaCompilerSupport withBootClassPath(List<File> newValue) {
        this.bootClassPath = newValue == null ? null : new ArrayList<>(newValue);
        return this;
    }

    /**
     * Sets the Java compliant version.
     * @param newValue the value
     * @return this
     */
    public BasicJavaCompilerSupport withCompliantVersion(String newValue) {
        this.compliantVersion = newValue;
        return this;
    }

    /**
     * Returns the source path.
     * @return the source path
     */
    public File getSourcePath() {
        return sourcePath;
    }

    /**
     * Returns the class path.
     * @return the class path
     */
    public List<File> getClassPath() {
        return classPath;
    }

    /**
     * Returns the destination path.
     * @return the destination path
     */
    public File getDestinationPath() {
        return destinationPath;
    }

    @Override
    protected String getCompliantVersion() {
        String result = compliantVersion;
        if (result == null) {
            return super.getCompliantVersion();
        }
        return result;
    }

    @Override
    protected boolean isCompileRequired() {
        return isCompileRequired(sourcePath);
    }

    private boolean isCompileRequired(File file) {
        String name = file.getName();
        if (name.startsWith(".")) { //$NON-NLS-1$
            return false;
        }
        if (file.isDirectory()) {
            for (File child : list(file)) {
                if (isCompileRequired(child)) {
                    return true;
                }
            }
        } else if (file.isFile()) {
            if (name.endsWith(JAVA_EXTENSION)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected OutputStream addResource(Location location) throws IOException {
        File file = new File(sourcePath, location.toPath(File.separatorChar)).getAbsoluteFile();
        if (file.exists()) {
            throw new IOException(MessageFormat.format(
                    "generating file already exists: {0}",
                    file));
        }
        File parent = file.getParentFile();
        if (parent.mkdirs() == false && parent.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to prepare a parent directory: {0}",
                    file));
        }
        return new FileOutputStream(file);
    }

    @Override
    protected JavaFileManager getJavaFileManager(
            JavaCompiler compiler,
            DiagnosticListener<JavaFileObject> listener) throws IOException {
        assert isCompileRequired();
        if (destinationPath.mkdirs() == false && destinationPath.isDirectory() == false) {
            throw new IOException(MessageFormat.format(
                    "failed to create Java compiler output directory: {0}",
                    destinationPath));
        }
        StandardJavaFileManager files = compiler.getStandardFileManager(
                listener,
                Locale.getDefault(),
                getEncoding());
        files.setLocation(StandardLocation.SOURCE_PATH, Arrays.asList(sourcePath));
        files.setLocation(StandardLocation.CLASS_OUTPUT, Arrays.asList(destinationPath));
        files.setLocation(StandardLocation.CLASS_PATH, classPath);
        List<File> boot = bootClassPath;
        if (boot != null) {
            files.setLocation(StandardLocation.PLATFORM_CLASS_PATH, boot);
        }
        return files;
    }

    @Override
    protected List<String> getCompilerOptions() {
        List<String> results = new ArrayList<>();
        Collections.addAll(results, "-proc:none"); //$NON-NLS-1$
        Collections.addAll(results, "-Xlint:all"); //$NON-NLS-1$
        Collections.addAll(results, "-Xlint:-options"); //$NON-NLS-1$
        return results;
    }

    @Override
    protected Iterable<? extends JavaFileObject> getSourceFiles(JavaFileManager fileManager) {
        assert isCompileRequired();
        assert fileManager instanceof StandardJavaFileManager;
        StandardJavaFileManager files = (StandardJavaFileManager) fileManager;
        List<File> sources = new ArrayList<>();
        collectSourceFiles(sources, sourcePath);
        return files.getJavaFileObjectsFromFiles(sources);
    }

    private void collectSourceFiles(List<File> sink, File file) {
        String name = file.getName();
        // skip "dot" files
        if (name.startsWith(".")) { //$NON-NLS-1$
            return;
        }
        if (file.isDirectory()) {
            for (File child : list(file)) {
                collectSourceFiles(sink, child);
            }
        } else if (file.isFile()) {
            if (name.endsWith(JAVA_EXTENSION)) {
                sink.add(file);
            }
        }
    }

    private static List<File> list(File file) {
        return Optional.ofNullable(file.listFiles())
                .map(Arrays::asList)
                .orElse(Collections.emptyList());
    }
}
