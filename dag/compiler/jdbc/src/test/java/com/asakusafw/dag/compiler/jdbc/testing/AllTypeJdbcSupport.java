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
package com.asakusafw.dag.compiler.jdbc.testing;

import java.lang.reflect.Modifier;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.runtime.value.ValueOption;
import com.asakusafw.windgate.core.vocabulary.DataModelJdbcSupport;

@SuppressWarnings("javadoc")
public class AllTypeJdbcSupport implements DataModelJdbcSupport<AllType> {

    private final Map<String, String> COLUMN_MAP = Stream.of(AllType.class.getMethods())
            .filter(m -> Modifier.isStatic(m.getModifiers()) == false)
            .filter(m -> m.getParameterCount() == 0)
            .filter(m -> ValueOption.class.isAssignableFrom(m.getReturnType()))
            .map(m -> PropertyName.of(m.getName()))
            .filter(n -> n.getWords().size() >= 3)
            .filter(n -> n.getWords().get(0).equals("get"))
            .filter(n -> n.getWords().get(n.getWords().size() - 1).equals("option"))
            .map(PropertyName::removeFirst)
            .map(PropertyName::removeLast)
            .map(n -> String.join("_", n.getWords()))
            .collect(Collectors.collectingAndThen(
                    Collectors.toMap(String::toUpperCase, String::toLowerCase),
                    Collections::unmodifiableMap));

    @Override
    public Class<AllType> getSupportedType() {
        return AllType.class;
    }

    @Override
    public Map<String, String> getColumnMap() {
        return COLUMN_MAP;
    }

    @Override
    public boolean isSupported(List<String> columnNames) {
        return COLUMN_MAP.keySet().containsAll(columnNames);
    }

    @Override
    public DataModelResultSet<AllType> createResultSetSupport(
            ResultSet resultSet, List<String> columnNames) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DataModelPreparedStatement<AllType> createPreparedStatementSupport(
            PreparedStatement statement, List<String> columnNames) {
        throw new UnsupportedOperationException();
    }
}
