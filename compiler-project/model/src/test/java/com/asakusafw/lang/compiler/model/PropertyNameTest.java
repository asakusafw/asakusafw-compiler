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
package com.asakusafw.lang.compiler.model;

import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.model.PropertyName.Option;

/**
 * Test for {@link PropertyName}.
 */
public class PropertyNameTest {

    /**
     * trivial case.
     */
    @Test
    public void simple() {
        PropertyName name = PropertyName.of("simple");
        assertThat(name.isEmpty(), is(false));
        assertThat(name.getWords(), is(words("simple")));
    }

    /**
     * lower snake case name.
     */
    @Test
    public void lower_snake_case() {
        PropertyName name = PropertyName.of("abc_de_fgh");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * upper snake case name.
     */
    @Test
    public void upper_snake_case() {
        PropertyName name = PropertyName.of("ABC_DE_FGH");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * lower camel case name.
     */
    @Test
    public void lower_camel_case() {
        PropertyName name = PropertyName.of("abcDeFgh");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * upper camel case name.
     */
    @Test
    public void upper_camel_case() {
        PropertyName name = PropertyName.of("AbcDeFgh");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * lower snake case name w/ trimming.
     */
    @Test
    public void trim_snake() {
        PropertyName name = PropertyName.of("_abc_de_fgh_");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * lower snake case name w/ trimming.
     */
    @Test
    public void trim_camel() {
        PropertyName name = PropertyName.of("_abcDeFgh_");
        assertThat(name.getWords(), is(words("abc,de,fgh")));
    }

    /**
     * empty name.
     */
    @Test
    public void empty_name() {
        PropertyName name = PropertyName.of("_");
        assertThat(name.isEmpty(), is(true));
    }

    /**
     * builds snake case name.
     */
    @Test
    public void to_snake() {
        PropertyName name = new PropertyName(words("hello,world"));
        assertThat(name.toName(), is("hello_world"));
    }

    /**
     * builds camel case name.
     */
    @Test
    public void to_camel() {
        PropertyName name = new PropertyName(words("hello,world"));
        assertThat(name.toMemberName(), is("helloWorld"));
    }

    /**
     * manipulates name.
     */
    @Test
    public void add() {
        PropertyName name = new PropertyName(words("hello"));
        PropertyName edit = name.addFirst("get").addLast("option");
        assertThat(edit, is(not(name)));
        assertThat(edit.getWords(), is(words("get,hello,option")));
    }

    /**
     * manipulates name.
     */
    @Test
    public void remove() {
        PropertyName name = new PropertyName(words("get,hello,option"));
        PropertyName edit = name.removeFirst().removeLast();
        assertThat(edit, is(not(name)));
        assertThat(edit.getWords(), is(words("hello")));
    }

    /**
     * w/ case-less word.
     */
    @Test
    public void caseless() {
        PropertyName name = PropertyName.of("code_100");
        assertThat(name.getWords(), is(words("code100")));
    }

    /**
     * w/ case-less word.
     */
    @Test
    public void caseless_suppress() {
        PropertyName name = PropertyName.of("code_100", Option.KEEP_CASELESS_WORDS);
        assertThat(name.getWords(), is(words("code,100")));
    }

    private List<String> words(String sequence) {
        List<String> results = new ArrayList<>();
        for (String w : sequence.split(",")) {
            if (w.isEmpty() == false) {
                results.add(w);
            }
        }
        return results;
    }
}
