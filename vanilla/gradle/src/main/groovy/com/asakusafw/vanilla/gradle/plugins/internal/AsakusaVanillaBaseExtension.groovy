/*
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
package com.asakusafw.vanilla.gradle.plugins.internal

/**
 * An extension object for the Asakusa Vanilla features.
 * This is only for internal use.
 * @since 0.4.0
 */
class AsakusaVanillaBaseExtension {

    /**
     * The Asakusa Vanilla libraries version.
     */
    String featureVersion

    /**
     * The core libraries version.
     */
    String coreVersion

    /**
     * The SDK libraries version.
     */
    String sdkVersion

    /**
     * The Hadoop version.
     */
    String hadoopVersion
}
