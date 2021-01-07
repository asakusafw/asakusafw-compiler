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
package com.asakusafw.dag.compiler.jdbc.windgate;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.NamePattern;
import com.asakusafw.lang.compiler.extension.windgate.DescriptionModel;
import com.asakusafw.lang.compiler.extension.windgate.WindGateConstants;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.TypeDescription;
import com.asakusafw.lang.compiler.model.info.ExternalInputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalOutputInfo;
import com.asakusafw.lang.compiler.model.info.ExternalPortInfo;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.Invariants;
import com.asakusafw.lang.utils.common.Optionals;
import com.asakusafw.lang.utils.common.Tuple;
import com.asakusafw.vocabulary.windgate.Constants;
import com.asakusafw.windgate.core.DriverScript;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport;
import com.asakusafw.windgate.core.vocabulary.JdbcProcess;

/**
 * Analyzes WindGate JDBC inputs/outputs.
 * @since 0.4.0
 */
public final class WindGateJdbcIoAnalyzer {

    /**
     * The compiler property key of profile name patterns which WindGate JDBC direct mode is enabled.
     * The pattern can contain commas to separate individual profile names,
     * and asterisks as wildcard character that represents zero or more any characters.
     */
    public static final String KEY_DIRECT = "windgate.jdbc.direct";

    static final Logger LOG = LoggerFactory.getLogger(WindGateJdbcIoAnalyzer.class);

    private WindGateJdbcIoAnalyzer() {
        return;
    }

    /**
     * Returns profile name pattern which WindGate JDBC direct mode is enabled.
     * @param options the compiler options
     * @return the name pattern
     */
    public static NamePattern getProfileNamePattern(CompilerOptions options) {
        Arguments.requireNonNull(options);
        return getProfileNamePattern(options.get(KEY_DIRECT, null));
    }

    /**
     * Returns profile name patterns from the expression.
     * @param expresssion the expression
     * @return the name pattern
     */
    public static NamePattern getProfileNamePattern(String expresssion) {
        if (expresssion == null) {
            return NamePattern.EMPTY;
        }
        return new NamePattern(expresssion.split(",")); //$NON-NLS-1$
    }

    /**
     * Analyzes WindGate JDBC input and returns the corresponded model object.
     * @param classLoader the class loader
     * @param info the target port info
     * @return the corresponded model object, or empty if the script is not a JDBC input script
     */
    public static Optional<WindGateJdbcInputModel> analyze(ClassLoader classLoader, ExternalInputInfo info) {
        Arguments.requireNonNull(classLoader);
        Arguments.requireNonNull(info);
        return extract(info)
                .flatMap(m -> input(classLoader, info.getDataModelClass(), m.getProfileName(), m.getDriverScript()));
    }

    /**
     * Analyzes WindGate JDBC output and returns the corresponded model object.
     * @param classLoader the class loader
     * @param info the target port info
     * @return the corresponded model object, or empty if the script is not a JDBC output script
     */
    public static Optional<WindGateJdbcOutputModel> analyze(ClassLoader classLoader, ExternalOutputInfo info) {
        Arguments.requireNonNull(classLoader);
        Arguments.requireNonNull(info);
        return extract(info)
                .flatMap(m -> output(classLoader, info.getDataModelClass(), m.getProfileName(), m.getDriverScript()));
    }

    private static Optional<DescriptionModel> extract(ExternalPortInfo info) {
        if (info.getModuleName().equals(WindGateConstants.MODULE_NAME) == false) {
            return Optional.empty();
        }
        try {
            DescriptionModel model = (DescriptionModel) info.getContents()
                    .resolve(DescriptionModel.class.getClassLoader());
            return Optional.of(model);
        } catch (ReflectiveOperationException e) {
            LOG.warn("failed to resolve WindGate model: {}", info, e);
            return Optional.empty();
        }
    }

    /**
     * Analyzes WindGate JDBC input and returns the corresponded model object.
     * @param classLoader the class loader
     * @param dataType the target data type
     * @param profileName the profile name
     * @param script the driver script
     * @return the corresponded model object, or empty if the script is not a JDBC input script
     */
    public static Optional<WindGateJdbcInputModel> input(
            ClassLoader classLoader, TypeDescription dataType,
            String profileName, DriverScript script) {
        Arguments.requireNonNull(classLoader);
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(script);
        if (isJdbc(script) == false) {
            return Optional.empty();
        }
        Map<String, String> conf = script.getConfiguration();
        String tableName = conf.get(JdbcProcess.TABLE.key());
        List<Tuple<String, PropertyName>> mappings = parseMappings(classLoader, conf);
        String condition = conf.get(JdbcProcess.CONDITION.key());
        Set<String> options = parseOptions(conf);
        return Optional.of(new WindGateJdbcInputModel(dataType, profileName, tableName, mappings)
                .withCondition(condition)
                .withOptions(options));
    }

    /**
     * Analyzes WindGate JDBC output and returns the corresponded model object.
     * @param classLoader the class loader
     * @param dataType the target data type
     * @param profileName the profile name
     * @param script the driver script
     * @return the corresponded model object, or empty if the script is not a JDBC output script
     */
    public static Optional<WindGateJdbcOutputModel> output(
            ClassLoader classLoader, TypeDescription dataType,
            String profileName, DriverScript script) {
        Arguments.requireNonNull(classLoader);
        Arguments.requireNonNull(dataType);
        Arguments.requireNonNull(profileName);
        Arguments.requireNonNull(script);
        if (isJdbc(script) == false) {
            return Optional.empty();
        }
        Map<String, String> conf = script.getConfiguration();
        String tableName = conf.get(JdbcProcess.TABLE.key());
        List<Tuple<String, PropertyName>> mappings = parseMappings(classLoader, conf);
        String truncate = conf.get(JdbcProcess.CUSTOM_TRUNCATE.key());
        Set<String> options = parseOptions(conf);
        return Optional.of(new WindGateJdbcOutputModel(dataType, profileName, tableName, mappings)
                .withCustomTruncate(truncate)
                .withOptions(options));
    }

    private static boolean isJdbc(DriverScript script) {
        return script.getResourceName().equals(Constants.JDBC_RESOURCE_NAME);
    }

    private static List<Tuple<String, PropertyName>> parseMappings(ClassLoader classLoader, Map<String, String> conf) {
        List<String> columns = Optionals.get(conf, JdbcProcess.COLUMNS.key())
                .map(WindGateJdbcIoAnalyzer::split)
                .orElseThrow(() -> new IllegalStateException());
        ClassDescription supportClass = Optionals.get(conf, JdbcProcess.JDBC_SUPPORT.key())
                .map(ClassDescription::new)
                .orElseThrow(() -> new IllegalStateException());
        try {
            DataModelJdbcSupport<?> support = supportClass.resolve(classLoader)
                    .asSubclass(DataModelJdbcSupport.class)
                    .newInstance();
            Map<String, String> map = support.getColumnMap();
            return columns.stream()
                    .map(s -> new Tuple<>(s, PropertyName.of(Invariants.requireNonNull(map.get(s), s))))
                    .collect(Collectors.toList());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(MessageFormat.format(
                    "failed to resolve JDBC support class: {0}",
                    supportClass.getClassName()), e);
        }
    }

    private static Set<String> parseOptions(Map<String, String> conf) {
        return Optionals.get(conf, JdbcProcess.OPTIONS.key())
                .map(s -> (Set<String>) new LinkedHashSet<>(split(s)))
                .orElse(Collections.emptySet());
    }

    private static List<String> split(String joined) {
        return Stream.of(joined.split(",")) //$NON-NLS-1$
                .map(String::trim)
                .filter(s -> s.isEmpty() == false)
                .collect(Collectors.toList());
    }
}
