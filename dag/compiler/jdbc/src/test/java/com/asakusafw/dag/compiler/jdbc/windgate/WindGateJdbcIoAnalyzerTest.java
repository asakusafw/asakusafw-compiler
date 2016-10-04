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
package com.asakusafw.dag.compiler.jdbc.windgate;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;

import org.junit.Test;

import com.asakusafw.dag.compiler.jdbc.JdbcDagCompilerTestRoot;
import com.asakusafw.dag.compiler.jdbc.testing.AllType;
import com.asakusafw.dag.compiler.jdbc.testing.AllTypeJdbcSupport;
import com.asakusafw.lang.compiler.api.CompilerOptions;
import com.asakusafw.lang.compiler.common.NamePattern;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.vocabulary.windgate.Constants;
import com.asakusafw.windgate.core.DriverScript;
import com.asakusafw.windgate.core.vocabulary.JdbcProcess;

/**
 * Test for {@link WindGateJdbcIoAnalyzer}.
 */
public class WindGateJdbcIoAnalyzerTest extends JdbcDagCompilerTestRoot {

    /**
     * profiles - simple.
     */
    @Test
    public void profiles() {
        CompilerOptions options = CompilerOptions.builder()
                .withProperty(WindGateJdbcIoAnalyzer.KEY_DIRECT, "testing")
                .build();
        NamePattern pattern = WindGateJdbcIoAnalyzer.getProfileNamePattern(options);
        assertThat(pattern.getAlternatives(), containsInAnyOrder("testing"));
    }

    /**
     * profiles - w/o property.
     */
    @Test
    public void profiles_missing() {
        CompilerOptions options = CompilerOptions.builder()
                .build();
        NamePattern pattern = WindGateJdbcIoAnalyzer.getProfileNamePattern(options);
        assertThat(pattern.getAlternatives(), hasSize(0));
    }

    /**
     * profiles -w/ multiple entries.
     */
    @Test
    public void profiles_multiple() {
        CompilerOptions options = CompilerOptions.builder()
                .withProperty(WindGateJdbcIoAnalyzer.KEY_DIRECT, "a, b, c")
                .build();
        NamePattern pattern = WindGateJdbcIoAnalyzer.getProfileNamePattern(options);
        assertThat(pattern.getAlternatives(), containsInAnyOrder("a", "b", "c"));
    }

    /**
     * profiles - w/ wildcard.
     */
    @Test
    public void profiles_wildcard() {
        CompilerOptions options = CompilerOptions.builder()
                .withProperty(WindGateJdbcIoAnalyzer.KEY_DIRECT, "test-*,*-test")
                .build();
        NamePattern pattern = WindGateJdbcIoAnalyzer.getProfileNamePattern(options);
        assertThat(pattern.getAlternatives(), containsInAnyOrder("test-*", "*-test"));
    }

    /**
     * input - simple.
     */
    @Test
    public void input() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME, new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT", "STRING"),
        });
        WindGateJdbcInputModel model = WindGateJdbcIoAnalyzer.input(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).get();
        assertThat(model.getProfileName(), is("testing"));
        assertThat(model.getDataType(), is(typeOf(AllType.class)));
        assertThat(model.getTableName(), is("ALL"));
        assertThat(model.getColumnMappings(), is(mappings("INT:int", "STRING:string")));
        assertThat(model.getCondition(), is(nullValue()));
        assertThat(model.getOptions(), hasSize(0));
    }

    /**
     * input - w/ options.
     */
    @Test
    public void input_options() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME, new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT"),
                JdbcProcess.CONDITION.key(), "STRING = 'HELLO'",
                JdbcProcess.OPTIONS.key(), String.join(",", "A", "B", "C"),
        });
        WindGateJdbcInputModel model = WindGateJdbcIoAnalyzer.input(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).get();
        assertThat(model.getProfileName(), is("testing"));
        assertThat(model.getDataType(), is(typeOf(AllType.class)));
        assertThat(model.getTableName(), is("ALL"));
        assertThat(model.getColumnMappings(), is(mappings("INT:int")));
        assertThat(model.getCondition(), is("STRING = 'HELLO'"));
        assertThat(model.getOptions(), containsInAnyOrder("A", "B", "C"));
    }

    /**
     * input - unsupported.
     */
    @Test
    public void input_unsupported() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME + "_INVALID", new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT", "STRING"),
        });
        WindGateJdbcInputModel model = WindGateJdbcIoAnalyzer.input(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).orElse(null);
        assertThat(model, is(nullValue()));
    }

    /**
     * output - simple.
     */
    @Test
    public void output() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME, new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT", "STRING"),
        });
        WindGateJdbcOutputModel model = WindGateJdbcIoAnalyzer.output(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).get();
        assertThat(model.getProfileName(), is("testing"));
        assertThat(model.getDataType(), is(typeOf(AllType.class)));
        assertThat(model.getTableName(), is("ALL"));
        assertThat(model.getColumnMappings(), is(mappings("INT:int", "STRING:string")));
        assertThat(model.getCustomTruncate(), is(nullValue()));
        assertThat(model.getOptions(), hasSize(0));
    }

    /**
     * output - w/ options.
     */
    @Test
    public void output_options() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME, new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT"),
                JdbcProcess.CUSTOM_TRUNCATE.key(), "DELETE ALL WHERE STRING = '1'",
                JdbcProcess.OPTIONS.key(), String.join(",", "A", "B", "C"),
                JdbcProcess.OPERATION.key(), JdbcProcess.OperationKind.INSERT_AFTER_TRUNCATE.value(),
        });
        WindGateJdbcOutputModel model = WindGateJdbcIoAnalyzer.output(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).get();
        assertThat(model.getProfileName(), is("testing"));
        assertThat(model.getDataType(), is(typeOf(AllType.class)));
        assertThat(model.getTableName(), is("ALL"));
        assertThat(model.getColumnMappings(), is(mappings("INT:int")));
        assertThat(model.getCustomTruncate(), is("DELETE ALL WHERE STRING = '1'"));
        assertThat(model.getOptions(), containsInAnyOrder("A", "B", "C"));
    }

    /**
     * output - unsupported.
     */
    @Test
    public void output_unsupported() {
        DriverScript script = script(Constants.JDBC_RESOURCE_NAME + "_INVALID", new String[] {
                JdbcProcess.JDBC_SUPPORT.key(), AllTypeJdbcSupport.class.getName(),
                JdbcProcess.TABLE.key(), "ALL",
                JdbcProcess.COLUMNS.key(), String.join(",", "INT", "STRING"),
        });
        WindGateJdbcOutputModel model = WindGateJdbcIoAnalyzer.output(
                getClass().getClassLoader(),
                Descriptions.typeOf(AllType.class),
                "testing",
                script).orElse(null);
        assertThat(model, is(nullValue()));
    }

    private static DriverScript script(String resource, String... pairs) {
        assert pairs.length % 2 == 0;
        Map<String, String> conf = new LinkedHashMap<>();
        for (int i = 0; i < pairs.length; i += 2) {
            conf.put(pairs[i + 0], pairs[i + 1]);
        }
        return new DriverScript(resource, conf);
    }
}
