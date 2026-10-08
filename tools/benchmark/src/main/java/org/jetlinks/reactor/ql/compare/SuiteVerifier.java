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
