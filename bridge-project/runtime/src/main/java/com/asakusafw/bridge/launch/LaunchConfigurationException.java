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
package com.asakusafw.bridge.launch;

/**
 * An exception occurred when launch configuration is not valid.
 */
public class LaunchConfigurationException extends Exception {

    private static final long serialVersionUID = 7571097045660270263L;

    /**
     * Creates a new instance.
     * @param message the message
     */
    public LaunchConfigurationException(String message) {
        super(message);
    }

    /**
     * Creates a new instance.
     * @param message the message
     * @param cause the cause of this exception
     */
    public LaunchConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
