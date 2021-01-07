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
package com.asakusafw.lang.compiler.optimizer.basic;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.ServiceLoader;

final class Util {

    private Util() {
        return;
    }

    public static <T> Collection<T> load(ClassLoader loader, Class<? extends T> serviceType) {
        ServiceLoader<? extends T> services = ServiceLoader.load(serviceType, loader);
        List<T> results = new ArrayList<>();
        for (T service : services) {
            results.add(service);
        }
        return results;
    }
}
