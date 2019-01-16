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
package com.asakusafw.dag.runtime.jdbc.operation;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.dag.runtime.jdbc.JdbcProfile;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * The JDBC adapter environment.
 * @since 0.4.0
 */
public class JdbcEnvironment {

    static final Logger LOG = LoggerFactory.getLogger(JdbcEnvironment.class);

    private final Map<String, JdbcProfile> profiles;

    /**
     * Creates a new instance.
     * @param profiles the JDBC profiles in this environment
     */
    public JdbcEnvironment(Collection<? extends JdbcProfile> profiles) {
        Arguments.requireNonNull(profiles);
        this.profiles = profiles.stream()
                .sequential()
                .collect(Collectors.collectingAndThen(
                        Collectors.toMap(
                                JdbcProfile::getName, Function.identity(),
                                (key, conflict) -> {
                                    throw new IllegalArgumentException(MessageFormat.format(
                                            "conflict JDBC profiles: {0}",
                                            key));
                                },
                                LinkedHashMap::new),
                        Collections::unmodifiableMap));
    }

    /**
     * Returns a JDBC profile.
     * @param profileName the profile name
     * @return the related profile, never {@code null}
     */
    public JdbcProfile getProfile(String profileName) {
        return findProfile(profileName)
                .orElseThrow(() -> new IllegalArgumentException(MessageFormat.format(
                        "unknown JDBC profile: {0}",
                        profileName)));
    }

    /**
     * Returns a JDBC profile.
     * @param profileName the target profile name
     * @return the corresponded profile, or empty if there is no such a profile
     */
    public Optional<JdbcProfile> findProfile(String profileName) {
        return Optionals.get(profiles, profileName);
    }
}
