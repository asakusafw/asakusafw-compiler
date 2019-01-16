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
package com.asakusafw.dag.runtime.jdbc.testing;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Calendar;

import com.asakusafw.dag.runtime.jdbc.PreparedStatementAdapter;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Optionals;

/**
 * JDBC adapter for {@link TimeModel}.
 */
public class TimeJdbcAdapter implements ResultSetAdapter<TimeModel>, PreparedStatementAdapter<TimeModel> {

    private final TimeModel buffer = new TimeModel();

    private final Calendar calendar = Calendar.getInstance();

    @SuppressWarnings("deprecation")
    @Override
    public TimeModel extract(ResultSet row) throws SQLException {
        TimeModel object = buffer;
        object.getKeyOption().modify(row.getInt(1));
        object.getDateValueOption().modify(Optionals.of(row.getDate(2))
                .map(JdbcUtil::fromDate)
                .orElse(0));
        if (row.wasNull()) {
            object.getDateValueOption().setNull();
        }
        object.getTimestampValueOption().modify(Optionals.of(row.getTimestamp(3))
                .map(JdbcUtil::fromTimestamp)
                .orElse(0L));
        if (row.wasNull()) {
            object.getTimestampValueOption().setNull();
        }
        return object;
    }

    @Override
    public void drive(PreparedStatement row, TimeModel object) throws SQLException {
        row.setInt(1, object.getKeyOption().get());
        if (object.getDateValueOption().isNull()) {
            row.setNull(2, Types.DATE);
        } else {
            JdbcUtil.setParameter(row, 2, object.getDateValueOption().get(), calendar);
        }
        if (object.getTimestampValueOption().isNull()) {
            row.setNull(3, Types.TIMESTAMP);
        } else {
            JdbcUtil.setParameter(row, 3, object.getTimestampValueOption().get(), calendar);
        }
    }
}
