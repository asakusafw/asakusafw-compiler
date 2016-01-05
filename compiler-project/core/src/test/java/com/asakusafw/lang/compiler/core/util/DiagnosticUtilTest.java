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
package com.asakusafw.lang.compiler.core.util;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.util.Arrays;

import org.junit.Test;

import com.asakusafw.lang.compiler.common.BasicDiagnostic;
import com.asakusafw.lang.compiler.common.Diagnostic.Level;
import com.asakusafw.runtime.core.BatchRuntime;

/**
 * Test for {@link DiagnosticUtil}.
 */
public class DiagnosticUtilTest {

    /**
     * artifact info.
     */
    @Test
    public void artifact_info() {
        String info = DiagnosticUtil.getArtifactInfo(BatchRuntime.class);
        assertThat(info, is(notNullValue()));
    }

    /**
     * object info.
     */
    @Test
    public void object_info_immediate() {
        String info = DiagnosticUtil.getObjectInfo("hello");
        assertThat(info, is(notNullValue()));
    }

    /**
     * object info.
     */
    @Test
    public void object_info_list() {
        String info = DiagnosticUtil.getObjectInfo(Arrays.asList("a", "b", "c"));
        assertThat(info, is(notNullValue()));
    }

    /**
     * object info.
     */
    @Test
    public void object_info_array() {
        String info = DiagnosticUtil.getObjectInfo(new Object[] { "a", "b", "c" });
        assertThat(info, is(notNullValue()));
    }

    /**
     * log - simple case.
     */
    @Test
    public void log() {
        DiagnosticUtil.log(new BasicDiagnostic(Level.INFO, "info"));
        DiagnosticUtil.log(new BasicDiagnostic(Level.WARN, "warn"));
        DiagnosticUtil.log(new BasicDiagnostic(Level.ERROR, "error"));
    }

    /**
     * log - w/ causal exceptions.
     */
    @Test
    public void log_w_causes() {
        DiagnosticUtil.log(new BasicDiagnostic(Level.INFO, "info", new Exception()));
        DiagnosticUtil.log(new BasicDiagnostic(Level.WARN, "warn", new Exception()));
        DiagnosticUtil.log(new BasicDiagnostic(Level.ERROR, "error", new Exception()));
    }
}
