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
package com.asakusafw.lang.compiler.extension.directio;

import java.text.MessageFormat;

import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;

/**
 * Utilities for Direct file I/O models.
 * @since 0.3.1
 */
public final class DirectFileIoModels {

    private DirectFileIoModels() {
        return;
    }

    /**
     * Returns whether or not the target external port represents a direct file input.
     * @param info the target external port info
     * @return {@code true} if the target external port represents a direct, otherwise {@code false}
     */
    public static boolean isSupported(ExternalInputInfo info) {
        return isSupported0(info);
    }

    /**
     * Returns whether or not the target external port represents a direct file output.
     * @param info the target external port info
     * @return {@code true} if the target external port represents a direct, otherwise {@code false}
     */
    public static boolean isSupported(ExternalOutputInfo info) {
        return isSupported0(info);
    }

    /**
     * Resolves an external port of direct file input and returns a model object which represents its operation.
     * @param info the target external port
     * @return the operation model
     * @throws IllegalArgumentException if the target port does not represent a valid direct file input
     * @see #isSupported(ExternalInputInfo)
     */
    public static DirectFileInputModel resolve(ExternalInputInfo info) {
        return resolve0(DirectFileInputModel.class, info);
    }

    /**
     * Resolves an external port of direct file output and returns a model object which represents its operation.
     * @param info the target external port
     * @return the operation model
     * @throws IllegalArgumentException if the target port does not represent a valid direct file output
     * @see #isSupported(ExternalOutputInfo)
     */
    public static DirectFileOutputModel resolve(ExternalOutputInfo info) {
        return resolve0(DirectFileOutputModel.class, info);
    }

    private static boolean isSupported0(ExternalPortInfo info) {
        return info.getModuleName().equals(DirectFileIoConstants.MODULE_NAME);
    }

    private static <T> T resolve0(Class<T> type, ExternalPortInfo info) {
        if (isSupported0(info) == false) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "unsupported direct file I/O info: {0}",
                    info));
        }
        try {
            Object contents = info.getContents().resolve(type.getClassLoader());
            if (type.isInstance(contents) == false) {
                throw new IllegalArgumentException(MessageFormat.format(
                        "unsupported direct file I/O info: {0}",
                        info));
            }
            return type.cast(contents);
        } catch (ReflectiveOperationException e) {
            throw new IllegalArgumentException(MessageFormat.format(
                    "unsupported direct file I/O info: {0}",
                    info), e);
        }
    }
}
