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
package com.asakusafw.lang.info;

import static org.hamcrest.MatcherAssert.*;
import static org.hamcrest.Matchers.*;

import java.io.IOException;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Ser/De for DSL information models.
 */
public final class InfoSerDe {

    private InfoSerDe() {
        return;
    }

    /**
     * Serializes the given object.
     * @param <T> the object type
     * @param type the object type
     * @param object the target object
     * @return the serialized data
     */
    public static <T> byte[] serialize(Class<T> type, T object) {
        ObjectMapper mapper = mapper();
        try {
            return mapper.writerFor(type).writeValueAsBytes(object);
        } catch (JsonProcessingException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Deserializes the given byte sequence.
     * @param <T> the object type
     * @param type the object type
     * @param bytes the serialized data
     * @return the deserialized object
     */
    public static <T> T deserialize(Class<T> type, byte[] bytes) {
        ObjectMapper mapper = mapper();
        try {
            return mapper.readerFor(type).readValue(bytes);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    /**
     * Validates whether ser/de keeps equivalence of the given object.
     * @param <T> the object type
     * @param type the object type
     * @param object the target object
     */
    public static <T> void checkRestore(Class<T> type, T object) {
        T restored = deserialize(type, serialize(type, object));
        assertThat(object.toString(), restored, equalTo(object));
    }

    private static ObjectMapper mapper() {
        return new ObjectMapper()
                .setSerializationInclusion(JsonInclude.Include.NON_ABSENT);
    }
}
