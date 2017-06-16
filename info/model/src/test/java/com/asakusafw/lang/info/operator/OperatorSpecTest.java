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
package com.asakusafw.lang.info.operator;

import java.util.Collections;

import org.junit.Test;

import com.asakusafw.lang.info.InfoSerDe;
import com.asakusafw.lang.info.operator.CoreOperatorSpec.CoreOperatorKind;
import com.asakusafw.lang.info.value.AnnotationInfo;
import com.asakusafw.lang.info.value.ClassInfo;

/**
 * Test for {@link OperatorSpec}.
 */
public class OperatorSpecTest {

    /**
     * core operators.
     */
    @Test
    public void core() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                CoreOperatorSpec.of(CoreOperatorKind.CHECKPOINT));
    }

    /**
     * user operators.
     */
    @Test
    public void user() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                UserOperatorSpec.of(
                        AnnotationInfo.of(
                                ClassInfo.of("com.example.MockOp"),
                                Collections.emptyMap()),
                        ClassInfo.of("com.example.Op"),
                        ClassInfo.of("com.example.OpImpl"),
                        "testing"));
    }

    /**
     * flow operators.
     */
    @Test
    public void flow() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                FlowOperatorSpec.of(ClassInfo.of("com.example.Flow")));
    }

    /**
     * input operators.
     */
    @Test
    public void input() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                InputOperatorSpec.of("port", ClassInfo.of("com.example.Port")));
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                InputOperatorSpec.of("port", null));
    }

    /**
     * output operators.
     */
    @Test
    public void output() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                OutputOperatorSpec.of("port", ClassInfo.of("com.example.Port")));
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                OutputOperatorSpec.of("port", null));
    }

    /**
     * marker operators.
     */
    @Test
    public void marker() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                MarkerOperatorSpec.get());
    }

    /**
     * custom operators.
     */
    @Test
    public void custom() {
        InfoSerDe.checkRestore(
                OperatorSpec.class,
                CustomOperatorSpec.of("testing"));
    }
}
