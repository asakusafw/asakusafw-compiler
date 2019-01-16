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
package com.asakusafw.lang.compiler.optimizer.testing;

import com.asakusafw.lang.compiler.model.graph.CustomOperator;
import com.asakusafw.lang.compiler.model.graph.Operator;

/**
 * Mock {@link CustomOperator}.
 */
public class MockCustomOperator extends CustomOperator {

    private final String category;

    private MockCustomOperator(String category) {
        this.category = category;
    }

    /**
     * Creates a new builder.
     * @param categoryTag the category tag
     * @return the created builder
     */
    public static Builder builder(String categoryTag) {
        return new Builder(new MockCustomOperator(categoryTag));
    }

    @Override
    public String getCategory() {
        return category;
    }

    @Override
    public Operator copy() {
        return copyAttributesTo(new MockCustomOperator(category));
    }

    /**
     * The builder.
     */
    public static class Builder extends AbstractBuilder<MockCustomOperator, Builder> {

        Builder(MockCustomOperator owner) {
            super(owner);
        }

        @Override
        protected Builder getSelf() {
            return this;
        }
    }
}
