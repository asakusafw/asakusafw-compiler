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
package com.asakusafw.lang.compiler.core.basic;

import static com.asakusafw.lang.compiler.model.description.Descriptions.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.asakusafw.lang.compiler.api.reference.DataModelReference;
import com.asakusafw.lang.compiler.api.reference.PropertyReference;
import com.asakusafw.lang.compiler.common.BasicExtensionContainer;
import com.asakusafw.lang.compiler.common.DiagnosticException;
import com.asakusafw.lang.compiler.core.adapter.DataModelLoaderAdapter;
import com.asakusafw.lang.compiler.core.adapter.DataModelProcessorAdapter;
import com.asakusafw.lang.compiler.model.PropertyName;
import com.asakusafw.lang.compiler.model.description.ClassDescription;
import com.asakusafw.lang.compiler.model.description.Descriptions;
import com.asakusafw.runtime.model.DataModel;
import com.asakusafw.runtime.model.PropertyOrder;
import com.asakusafw.runtime.value.IntOption;
import com.asakusafw.runtime.value.StringOption;
import com.asakusafw.runtime.value.ValueOption;

/**
 * Test for {@link BasicDataModelProcessor}.
 */
public class BasicDataModelProcessorTest {

    /**
     * simple case.
     */
    @Test
    public void simple() {
        DataModelLoaderAdapter loader = newInstance();
        DataModelReference ref = loader.load(Descriptions.typeOf(Simple.class));
        assertThat(loader.load(Descriptions.typeOf(Simple.class)), is(ref));

        assertThat(ref.getDeclaration(), is(classOf(Simple.class)));
        assertThat(ref.getProperties(), hasSize(1));

        PropertyReference value = ref.findProperty(PropertyName.of("value"));
        assertThat(value, is(notNullValue()));
        assertThat(value.getOwner(), is(ref));
        assertThat(value.getName(), is(PropertyName.of("value")));
        assertThat(value.getType(), is((Object) classOf(IntOption.class)));
        assertThat(value.getDeclaration().getName(), is("getValueOption"));
    }

    /**
     * properties are sorted.
     */
    @Test
    public void sorted() {
        DataModelLoaderAdapter loader = newInstance();
        DataModelReference ref = loader.load(Descriptions.typeOf(Sorted.class));

        List<PropertyReference> properties = new ArrayList<>(ref.getProperties());
        assertThat(properties, hasSize(4));

        assertThat(properties.get(0).getName(), is(PropertyName.of("a")));
        assertThat(properties.get(1).getName(), is(PropertyName.of("b")));
        assertThat(properties.get(2).getName(), is(PropertyName.of("c")));
        assertThat(properties.get(3).getName(), is(PropertyName.of("d")));
    }

    /**
     * missing data model class.
     */
    @Test(expected = DiagnosticException.class)
    public void class_not_found() {
        DataModelLoaderAdapter loader = newInstance();
        loader.load(new ClassDescription("___MISSING___"));
    }

    /**
     * unsupported data model class.
     */
    @Test(expected = DiagnosticException.class)
    public void class_unsupported() {
        DataModelLoaderAdapter loader = newInstance();
        loader.load(classOf(String.class));
    }

    /**
     * missing data model properties.
     */
    @Test(expected = DiagnosticException.class)
    public void missing_properties() {
        DataModelLoaderAdapter loader = newInstance();
        loader.load(classOf(MissingProperties.class));
    }

    private DataModelLoaderAdapter newInstance() {
        return new DataModelLoaderAdapter(
                new BasicDataModelProcessor(),
                new DataModelProcessorAdapter(getClass().getClassLoader(), new BasicExtensionContainer()));
    }

    private static abstract class Abstract<T extends Abstract<T>> implements DataModel<T> {

        Abstract() {
            return;
        }

        @Override
        public void copyFrom(T other) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void reset() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * simple model.
     */
    @SuppressWarnings("unused")
    private static final class Simple extends Abstract<Simple> {

        public int getKey() {
            throw new UnsupportedOperationException();
        }

        public int getValue() {
            throw new UnsupportedOperationException();
        }

        public IntOption getValueOption() {
            throw new UnsupportedOperationException();
        }

        public String getInvalidOption() {
            throw new UnsupportedOperationException();
        }

        public ValueOption<?> getBaseOption() {
            throw new UnsupportedOperationException();
        }

        public static StringOption getClassFieldOption() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * ordered model.
     */
    @PropertyOrder({ "a", "b", "c", "d" })
    @SuppressWarnings("unused")
    private static final class Sorted extends Abstract<Sorted> {

        public IntOption getBOption() {
            throw new UnsupportedOperationException();
        }

        public IntOption getCOption() {
            throw new UnsupportedOperationException();
        }

        public IntOption getDOption() {
            throw new UnsupportedOperationException();
        }

        public IntOption getAOption() {
            throw new UnsupportedOperationException();
        }
    }

    /**
     * missing properties.
     */
    @PropertyOrder({ "a", "b", "c", "d" })
    private static final class MissingProperties extends Abstract<MissingProperties> {
        // no members
    }
}
