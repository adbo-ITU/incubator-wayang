package com.advanceddatasystems.app;

import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.core.api.Configuration;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.core.plan.wayangplan.WayangPlan;
import org.apache.wayang.core.util.ReflectionUtils;
import org.apache.wayang.java.Java;
import org.apache.wayang.java.platform.JavaPlatform;
import org.apache.wayang.spark.Spark;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

public class App {

    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 1) {
            System.err.println("Usage: <input file URL>");
            System.exit(1);
        }

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withUdfJarOf(App.class);

        Collection<Tuple2<String, Integer>> wordcounts = planBuilder
                .readParquet(args[1]).withName("Load file")

                /* Split each line by non-word characters */
                .flatMap(line -> Arrays.asList(line.split("\\W+")))
                .withSelectivity(1, 100, 0.9)
                .withName("Split words")

                /* Filter empty tokens */
                .filter(token -> !token.isEmpty())
                .withName("Filter empty words")

                /* Attach counter to each word */
                .map(word -> new Tuple2<>(word.toLowerCase(), 1)).withName("To lower case, add counter")

                // Sum up counters for every word.
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")

                /* Execute the plan and collect the results */
                .collect();


        System.out.printf("Found %d words:\n", wordcounts.size());
        wordcounts.forEach(wc -> System.out.printf("%dx %s\n", wc.field1, wc.field0));
    }
}


