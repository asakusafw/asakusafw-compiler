/**
 * Copyright 2011-2017 Asakusa Framework Team.
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
package com.asakusafw.dag.runtime.jdbc.basic;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.asakusafw.dag.api.processor.ObjectReader;
import com.asakusafw.dag.runtime.jdbc.ResultSetAdapter;
import com.asakusafw.dag.runtime.jdbc.util.JdbcUtil;
import com.asakusafw.lang.utils.common.Arguments;
import com.asakusafw.lang.utils.common.InterruptibleIo;

class BasicFetchCursor implements ObjectReader {

    private final ResultSet cursor;

    private final ResultSetAdapter<Object> adapter;

    private final InterruptibleIo resource;

    @SuppressWarnings("unchecked")
    BasicFetchCursor(ResultSet cursor, ResultSetAdapter<?> adapter, InterruptibleIo resource) {
        Arguments.requireNonNull(cursor);
        Arguments.requireNonNull(adapter);
        this.cursor = cursor;
        this.adapter = (ResultSetAdapter<Object>) adapter;
        this.resource = resource;
    }

    @Override
    public boolean nextObject() throws IOException {
        try {
            return cursor.next();
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    @Override
    public Object getObject() throws IOException {
        try {
            return adapter.extract(cursor);
        } catch (SQLException e) {
            throw JdbcUtil.wrap(e);
        }
    }

    @Override
    public void close() throws IOException, InterruptedException {
        if (resource != null) {
            resource.close();
        }
    }
}
