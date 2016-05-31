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
package com.asakusafw.lang.compiler.extension.hive;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.directio.hive.info.TableInfo;
import com.asakusafw.lang.compiler.model.description.ClassDescription;

final class Util {

    static final Logger LOG = LoggerFactory.getLogger(Util.class);

    private static final ClassDescription REQUIRED_CLASS =
            new ClassDescription("com.asakusafw.directio.hive.info.TableInfo"); //$NON-NLS-1$

    private Util() {
        return;
    }

    static void checkDependencies(ClassLoader loader) {
        LOG.debug("checking if hive dependencies are available");
        try {
            REQUIRED_CLASS.resolve(loader);
            LOG.debug("hive dependencies are available: {}", REQUIRED_CLASS);
        } catch (ClassNotFoundException e) {
            // if dependencies are not available, then the application cannot contain hive related features
            LOG.debug("hive dependencies are not available: {}", REQUIRED_CLASS, e);
        }
    }

    static <T extends TableInfo.Provider> List<T> normalize(List<T> elements) {
        if (elements.size() <= 1) {
            return elements;
        }
        Set<T> saw = new HashSet<>();
        List<T> normalized = new ArrayList<>();
        for (T element : elements) {
            if (saw.contains(element)) {
                continue;
            }
            saw.add(element);
            normalized.add(element);
        }
        Collections.sort(normalized, new Comparator<T>() {
            @Override
            public int compare(T o1, T o2) {
                return o1.getSchema().getName().compareTo(o2.getSchema().getName());
            }
        });
        return normalized;
    }
}
