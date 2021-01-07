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
package com.asakusafw.lang.utils.common;

import java.util.concurrent.Callable;

/**
 * Utilities for assertions.
 * @since 0.4.0
 */
public final class AssertUtil {

    private AssertUtil() {
        return;
    }

    /**
     * Asserts that the callable throws an exception.
     * @param callable the target callable
     * @return the thrown exception
     * @throws AssertionError if the callable does not throw any exceptions
     */
    public static Exception catching(Callable<?> callable) {
        try {
            callable.call();
            throw new AssertionError("exception required");
        } catch (Exception e) {
            return e;
        }
    }

    /**
     * Asserts that the runnable throws an exception.
     * @param runnable the target runnable
     * @return the thrown exception
     * @throws AssertionError if the callable does not throw any exceptions
     */
    public static Exception catching(RunnableWithException<?> runnable) {
        try {
            runnable.run();
            throw new AssertionError("exception required");
        } catch (Exception e) {
            return e;
        }
    }
}
