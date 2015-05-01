/**
 * Copyright 2011-2015 Asakusa Framework Team.
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

import java.text.MessageFormat;
import java.util.Collections;
import java.util.Iterator;

/**
 * An abstract implementation of {@link CompositeElement}.
 * @param <E> the element type
 */
public abstract class AbstractCompositeElement<E> implements CompositeElement<E> {

    @Override
    public Iterator<E> iterator() {
        return Collections.unmodifiableCollection(getElements()).iterator();
    }

    @Override
    public String toString() {
        if (getElements().isEmpty()) {
            return "NULL"; //$NON-NLS-1$
        }
        return MessageFormat.format(
                "Composite{0}", //$NON-NLS-1$
                getElements());
    }
}
