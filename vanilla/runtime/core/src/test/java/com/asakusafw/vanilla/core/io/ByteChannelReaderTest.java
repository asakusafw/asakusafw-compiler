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
package com.asakusafw.vanilla.core.io;

import static com.asakusafw.vanilla.core.testing.BufferTestUtil.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.io.File;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * Test for {@link ByteChannelReader}.
 */
public class ByteChannelReaderTest {

    /**
     * temporary folder.
     */
    @Rule
    public final TemporaryFolder folder = new TemporaryFolder();

    /**
     * simple case.
     * @throws Exception if failed
     */
    @Test
    public void simple() throws Exception {
        String value = "Hello, world!";
        File file = folder.newFile();
        try (WritableByteChannel channel = Files.newByteChannel(file.toPath(), StandardOpenOption.WRITE)) {
            channel.write(buffer(value));
        }
        try (DataReader reader = ByteChannelReader.open(file.toPath())) {
            assertThat(reader.getBuffer(), is(nullValue()));
            assertThat(read(reader), is(value));
        }
    }
}
