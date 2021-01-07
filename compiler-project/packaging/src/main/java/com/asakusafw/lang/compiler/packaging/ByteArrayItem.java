/**
 * Copyright 2011-2021 Asakusa Framework Team.
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
package com.asakusafw.lang.compiler.packaging;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.MessageFormat;
import java.util.Arrays;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceItem} using array of bytes.
 */
public class ByteArrayItem implements ResourceItem {

    private final Location location;

    private final byte[] contents;

    /**
     * Creates a new instance.
     * @param location the resource location
     * @param contents the resource contents
     */
    public ByteArrayItem(Location location, byte[] contents) {
        this.location = location;
        this.contents = contents.clone();
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public InputStream openResource() {
        return new ByteArrayInputStream(contents);
    }

    /**
     * Returns a copy of the resource contents.
     * @return a copy of the resource contents
     */
    public byte[] getContents() {
        return contents.clone();
    }

    @Override
    public void writeTo(OutputStream output) throws IOException {
        output.write(contents);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + location.hashCode();
        result = prime * result + Arrays.hashCode(contents);
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ByteArrayItem other = (ByteArrayItem) obj;
        if (!location.equals(other.location)) {
            return false;
        }
        if (!Arrays.equals(contents, other.contents)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "ByteArrayItem({0}=>{1}bytes)", //$NON-NLS-1$
                location,
                contents.length);
    }
}
