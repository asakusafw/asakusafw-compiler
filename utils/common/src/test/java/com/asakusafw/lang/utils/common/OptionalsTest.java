/**
 * Copyright 2011-2018 Asakusa Framework Team.
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
package com.asakusafw.lang.utils.common;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

/**
 * Test for {@link Optionals}.
 */
public class OptionalsTest {

    /**
     * optional.
     */
    @Test
    public void optional_present() {
        assertThat(Optionals.optional("OK").orElse(null), is("OK"));
    }

    /**
     * optional.
     */
    @Test
    public void optional_missing() {
        assertThat(Optionals.optional(null).orElse(null), is(nullValue()));
    }

    /**
     * get map value.
     */
    @Test
    public void map_present() {
        Map<String, String> map = new HashMap<>();
        map.put("K", "V");
        assertThat(Optionals.get(map, "K").orElse(null), is("V"));
    }

    /**
     * get map value.
     */
    @Test
    public void map_missing() {
        Map<String, String> map = new HashMap<>();
        map.put("K", "V");
        assertThat(Optionals.get(map, "?").orElse(null), is(nullValue()));
    }
}
