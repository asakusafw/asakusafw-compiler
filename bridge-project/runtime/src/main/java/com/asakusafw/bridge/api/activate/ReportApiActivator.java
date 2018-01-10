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
package com.asakusafw.bridge.api.activate;

import com.asakusafw.bridge.api.Report;
import com.asakusafw.runtime.core.api.ApiStub;
import com.asakusafw.runtime.core.api.ReportApi;

/**
 * Activates {@link Report}.
 * @since 0.4.0
 */
public class ReportApiActivator implements ApiActivator {

    private static final ReportApi API = new ReportApi() {
        @Override
        public void info(String message, Throwable throwable) {
            Report.info(message, throwable);
        }
        @Override
        public void info(String message) {
            Report.info(message);
        }
        @Override
        public void warn(String message, Throwable throwable) {
            Report.warn(message, throwable);
        }
        @Override
        public void warn(String message) {
            Report.warn(message);
        }
        @Override
        public void error(String message, Throwable throwable) {
            Report.error(message, throwable);
        }
        @Override
        public void error(String message) {
            Report.error(message);
        }
    };

    @Override
    public ApiStub.Reference<ReportApi> activate() {
        return com.asakusafw.runtime.core.Report.getStub().activate(API);
    }
}
