/*
 * Copyright 2025 JetLinks https://www.jetlinks.cn
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jetlinks.reactor.ql.compare;

import org.jetlinks.reactor.ql.DurationIntervalWorkloadBenchmark;
import org.jetlinks.reactor.ql.LayeredGroupHavingBenchmark;
import org.jetlinks.reactor.ql.NumericTextWorkloadBenchmark;
import org.jetlinks.reactor.ql.WideSqlWorkloadBenchmark;

/** Runs only frozen setup/oracles; this is not a benchmark measurement. */
public final class SuiteVerifier {
    public static void main(String[] args) {
        String which=args.length==0?"":args[0];
        System.out.println("ENGINE_SOURCE=" + org.jetlinks.reactor.ql.ReactorQL.class.getProtectionDomain().getCodeSource().getLocation());
        if (which.startsWith("wide:")) new WideSqlWorkloadBenchmark().setupCase(which.substring(5));
        else if (which.startsWith("numeric:")) new NumericTextWorkloadBenchmark().setupCase(which.substring(8));
        else if (which.startsWith("duration:")) new DurationIntervalWorkloadBenchmark().setupCase(which.substring(9));
        else if (which.startsWith("layered:")) new LayeredGroupHavingBenchmark().setupCase(which.substring(8));
        else if (which.startsWith("high:")) {
            CommonBaselineComparisonBenchmark.HighState state = new CommonBaselineComparisonBenchmark.HighState();
            state.valuesPerKey = Integer.parseInt(which.substring(5)); state.setup();
        } else new CommonBaselineComparisonBenchmark.CoreState().setupCase(which);
        System.out.println("COMMON_SUITE_VERIFIED " + which);
    }
}
