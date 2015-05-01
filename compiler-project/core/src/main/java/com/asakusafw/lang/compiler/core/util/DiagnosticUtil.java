/**
 * Copyright 2011-2015 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.core.util;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.common.Diagnostic;
import com.asakusafw.lang.compiler.packaging.ResourceUtil;

/**
 * Utilities for {@link Diagnostic}.
 */
public final class DiagnosticUtil {

    static final Logger LOG = LoggerFactory.getLogger(DiagnosticUtil.class);

    static final String MANIFEST_ARTIFACT_ID = "Artifact-Id"; //$NON-NLS-1$

    private static final String MANIFEST_IMPLEMENTATION_TITLE = "Implementation-Title"; //$NON-NLS-1$

    private static final String MANIFEST_IMPLEMENTATION_VERSION = "Implementation-Version"; //$NON-NLS-1$

    private DiagnosticUtil() {
        return;
    }

    /**
     * Returns the artifact information as string.
     * @param aClass a member class of the target artifact
     * @return the artifact information (never null)
     */
    public static String getArtifactInfo(Class<?> aClass) {
        File library = ResourceUtil.findLibraryByClass(aClass);
        return getArtifactInfo(library);
    }

    /**
     * Returns the detail object information as string.
     * @param object the target object
     * @return the object information (never null)
     */
    public static String getObjectInfo(Object object) {
        if (object == null) {
            return "N/A"; //$NON-NLS-1$
        } else if (object instanceof Iterable<?>) {
            List<String> results = new ArrayList<>();
            for (Object o : (Iterable<?>) object) {
                results.add(getObjectInfo(o));
            }
            return results.toString();
        } else if (object instanceof Object[]) {
            return getObjectInfo(Arrays.asList((Object[]) object));
        } else {
            String self = getSimpleObjectInfo(object);
            File library = ResourceUtil.findLibraryByClass(object.getClass());
            String artifact = getArtifactInfo(library);
            return String.format("%s@%s", self, artifact); //$NON-NLS-1$
        }
    }

    private static String getSimpleObjectInfo(Object object) {
        Class<?> aClass = object.getClass();
        try {
            // use toString() only if the target element has explicit one
            Method method = aClass.getMethod("toString"); //$NON-NLS-1$
            if (method.getDeclaringClass() != Object.class) {
                return object.toString();
            }
        } catch (NoSuchMethodException e) {
            LOG.debug("{} may not have explicit toString() method", aClass.getName()); //$NON-NLS-1$
        }
        return aClass.getSimpleName();
    }

    private static String getArtifactInfo(File file) {
        if (file == null || file.isFile() == false) {
            return "N/A"; //$NON-NLS-1$
        }
        try (JarFile jar = new JarFile(file)) {
            Manifest manifest = jar.getManifest();
            if (manifest != null) {
                String info = getArtifactInfo(manifest);
                if (info != null) {
                    return info;
                }
            }
        } catch (IOException e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(MessageFormat.format(
                        "target artifact is not a valid ZIP file: {0}", //$NON-NLS-1$
                        file), e);
            }
        }
        return file.getName();
    }

    private static String getArtifactInfo(Manifest manifest) {
        Attributes main = manifest.getMainAttributes();
        if (main == null) {
            // may not occur
            return null;
        }
        String id = main.getValue(MANIFEST_ARTIFACT_ID);
        if (id != null) {
            return id;
        }
        String title = main.getValue(MANIFEST_IMPLEMENTATION_TITLE);
        String version = main.getValue(MANIFEST_IMPLEMENTATION_VERSION);
        if (title != null && version != null) {
            return String.format("%s-%s", title, version); //$NON-NLS-1$
        }
        return null;
    }

    /**
     * Logs a target diagnostic object.
     * @param diagnostic the target diagnostic object
     */
    public static void log(Diagnostic diagnostic) {
        log(LOG, diagnostic);
    }

    /**
     * Logs a target diagnostic object.
     * @param logger the target logger
     * @param diagnostic the target diagnostic object
     */
    public static void log(Logger logger, Diagnostic diagnostic) {
        switch (diagnostic.getLevel()) {
        case ERROR:
            if (diagnostic.getException() == null) {
                logger.error(diagnostic.getMessage());
            } else {
                logger.error(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        case WARN:
            if (diagnostic.getException() == null) {
                logger.warn(diagnostic.getMessage());
            } else {
                logger.warn(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        case INFO:
            if (diagnostic.getException() == null) {
                logger.info(diagnostic.getMessage());
            } else {
                logger.info(diagnostic.getMessage(), diagnostic.getException());
            }
            break;
        default:
            throw new AssertionError(diagnostic);
        }
    }
}
