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
package com.asakusafw.lang.compiler.packaging;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.text.MessageFormat;

import com.asakusafw.lang.compiler.common.Location;

/**
 * An implementation of {@link ResourceItem} using URL.
 */
public class UrlItem implements ResourceItem {

    private final Location location;

    private final URL url;

    private final String label;

    /**
     * Creates a new instance.
     * @param location the file location
     * @param url the target URL
     */
    public UrlItem(Location location, URL url) {
        this.location = location;
        this.url = url;
        this.label = url.toExternalForm();
    }

    @Override
    public Location getLocation() {
        return location;
    }

    @Override
    public InputStream openResource() throws IOException {
        return url.openStream();
    }

    @Override
    public void writeTo(OutputStream output) throws IOException {
        try (InputStream input = openResource()) {
            ResourceUtil.copy(input, output);
        }
    }

    /**
     * Returns the URL.
     * @return the URL
     */
    public URL getUrl() {
        return url;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + location.hashCode();
        result = prime * result + label.hashCode();
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
        UrlItem other = (UrlItem) obj;
        if (!location.equals(other.location)) {
            return false;
        }
        if (!label.equals(other.label)) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return MessageFormat.format(
                "UrlItem({0}=>{1})", //$NON-NLS-1$
                location,
                label);
    }
}
