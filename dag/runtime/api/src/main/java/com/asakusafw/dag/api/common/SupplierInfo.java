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
package com.asakusafw.dag.api.common;

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Supplier;

import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;

/**
 * Represents {@link Supplier} reference.
 * @since 0.4.0
 */
@FunctionalInterface
public interface SupplierInfo extends Serializable {

    /**
     * Creates a new instance.
     * @param className the class name of target object or its supplier
     * @return the created instance
     */
    static SupplierInfo of(String className) {
        return new SupplierInfo.Basic(className);
    }

    /**
     * Creates a new supplier instance.
     * @param loader the class loader
     * @return the created supplier
     * @throws IllegalStateException if error was occurred while creating supplier instance
     */
    Supplier<?> newInstance(ClassLoader loader);

    /**
     * A basic implementation of {@link SupplierInfo}.
     * @since 0.4.0
     */
    class Basic implements SupplierInfo {

        private static final long serialVersionUID = 1L;

        private final String className;

        /**
         * Creates a new instance.
         * @param className the class name of target object or its supplier
         */
        public Basic(String className) {
            Arguments.requireNonNull(className);
            this.className = className;
        }

        @Override
        public Supplier<?> newInstance(ClassLoader loader) {
            Class<?> aClass = Invariants.safe(() -> loader.loadClass(className));
            if (Supplier.class.isAssignableFrom(aClass)) {
                return Invariants.safe(() -> (Supplier<?>) aClass.newInstance());
            } else {
                return () -> Invariants.safe(aClass::newInstance);
            }
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + Objects.hashCode(className);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Basic other = (Basic) obj;
            if (!Objects.equals(className, other.className)) {
                return false;
            }
            return true;
        }

        @Override
        public String toString() {
            return className;
        }
    }
}
