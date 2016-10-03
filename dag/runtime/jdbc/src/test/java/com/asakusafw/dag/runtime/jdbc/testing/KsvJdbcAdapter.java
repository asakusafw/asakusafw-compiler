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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;

/**
 * JDBC adapter for {@link KsvModel}.
 */
public class KsvJdbcAdapter implements ResultSetAdapter<KsvModel>, PreparedStatementAdapter<KsvModel> {

    private final KsvModel buffer = new KsvModel();

    @Override
    public KsvModel extract(ResultSet row) throws SQLException {
        KsvModel object = buffer;
        object.setKey(row.getLong(1));
        object.setSort(row.getBigDecimal(2));
        object.setValue(row.getString(3));
        return object;
    }

    @Override
    public void drive(PreparedStatement row, KsvModel object) throws SQLException {
        row.setLong(1, object.getKey());
        row.setBigDecimal(2, object.getSort());
        row.setString(3, object.getValue());
    }
}
