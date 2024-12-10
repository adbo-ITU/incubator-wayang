/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.wayang.apps.parquet;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class ParquetTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length < 2) {
            System.err.println("Usage: ParquetTest <bench-dir-path> <workload> [filter]");
            System.exit(1);
        }

        String benchDir = args[0];
        String workload = args[1];
        String filter = args.length > 2 ? args[2] : null;
        Collection<BenchConf> benchConfs = BenchConf.generateBenchmarksFromDir(benchDir, workload);

        if (filter != null) {
            System.out.println("Filter: " + filter);
        }

        JsonArray benchOutput = new JsonArray();

        for (int i = 0; i < 4; i++) {
            for (BenchConf benchConf : benchConfs) {
                System.out.printf("%nBenchConf %s with proj: %b%n", benchConf.inputPath, benchConf.shouldUseProjection);

                if (filter != null && !filter.isEmpty() && !benchConf.inputPath.contains(filter)) {
                    System.out.println("Skipping due to filter..");
                    continue;
                }

                BenchmarkResult result = run(benchConf);

                JsonObject benchItem = new JsonObject();
                benchItem.addProperty("path", benchConf.inputPath);
                benchItem.addProperty("workload", benchConf.workload);
                benchItem.addProperty("projected", benchConf.shouldUseProjection);
                benchItem.addProperty("numRecords", result.numRecords);
                benchItem.addProperty("iteration", i);

                System.out.println("\nMeasurements:");
                for (Measurement m : result.experiment.getMeasurements()) {
                    System.out.println("Measurement: " + m);

                    if (m.getId().equals("Execution")) {
                        benchItem.addProperty("executionTimeMillis", ((TimeMeasurement) m).getMillis());
                        benchItem.addProperty("executionTimePretty", TimeMeasurement.formatDuration(((TimeMeasurement) m).getMillis()));
                    }
                }
                System.out.println();

                benchOutput.add(benchItem);

                System.out.printf("Processed %d records. Results:\n", result.numRecords);
                result.results.forEach(res -> System.out.printf("%s\n", res.toString()));

                System.out.println("Intermediate bench output:");
                System.out.println(benchOutput);
            }

            System.out.println("\nBenchmark data:");
            System.out.println(benchOutput);
        }
    }

    private static BenchmarkResult run(BenchConf benchConf) {
        Experiment experiment = new Experiment("parquet-bench-exp", new Subject("parquet-bench", "v0.1"));

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withExperiment(experiment)
                .withUdfJarOf(ParquetTest.class);

        AtomicLong numRecords = new AtomicLong();
        Collection<Tuple2<String, Integer>> results;

        switch (benchConf.workload) {
            case "ssb":
                if (benchConf.inputPath.endsWith(".parquet")) {
                    results = runParquetSsb(benchConf, planBuilder, numRecords);
                } else if (benchConf.inputPath.endsWith(".csv")) {
                    results = runCsvSsb(benchConf, planBuilder, numRecords);
                } else {
                    throw new RuntimeException("Unknown file format: " + benchConf.inputPath);
                }
                break;
            case "yelp":
                if (benchConf.inputPath.endsWith(".parquet")) {
                    results = runParquetYelp(benchConf, planBuilder, numRecords);
                } else if (benchConf.inputPath.endsWith(".csv")) {
                    results = runCsvYelp(benchConf, planBuilder, numRecords);
                } else {
                    throw new RuntimeException("Unknown file format: " + benchConf.inputPath);
                }
                break;
            default:
                throw new IllegalArgumentException("Unknown workload: " + benchConf.workload);
        }

        return new BenchmarkResult(benchConf, experiment, numRecords.get(), results);
    }

    private static Collection<Tuple2<String, Integer>> runParquetSsb(BenchConf benchConf, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        Schema projection = benchConf.shouldUseProjection
                ? SchemaBuilder.record("ParquetProjection")
                .fields()
                .optionalString("lo_shipmode")
                .endRecord()
                : null;

        return planBuilder
                .readParquet(benchConf.inputPath, projection).withName("Load parquet file")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(r -> new Tuple2<>(r.get("lo_shipmode").toString(), 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();
    }

    private static Collection<Tuple2<String, Integer>> runCsvSsb(BenchConf benchConf, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        return planBuilder
                .readTextFile(benchConf.inputPath).withName("Load text file")
                .filter(row -> !row.startsWith("lo_orderkey")).withName("Remove headers").withName("Remove headers")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(line -> line.split(","))
                .map(r -> new Tuple2<>(r[16], 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();
    }

    private static Collection<Tuple2<String, Integer>> runParquetYelp(BenchConf benchConf, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        Schema projection = benchConf.shouldUseProjection
                ? SchemaBuilder.record("ParquetProjection")
                .fields()
                .optionalLong("label")
                .optionalString("text")
                .endRecord()
                : null;

        Collection<Tuple2<Long, Integer>> tmpRes = planBuilder
                .readParquet(benchConf.inputPath, projection).withName("Load parquet file")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(r -> new Tuple2<>(((Long) r.get("label")), 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();

        Collection<Tuple2<String, Integer>> res = new ArrayList<>();
        for (Tuple2<Long, Integer> r : tmpRes) {
            res.add(new Tuple2<>(r.getField0().toString(), r.getField1()));
        }
        return res;
    }

    private static Collection<Tuple2<String, Integer>> runCsvYelp(BenchConf benchConf, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        Collection<Tuple2<Long, Integer>> tmpRes = planBuilder
                .readTextFile(benchConf.inputPath).withName("Load text file")
                .filter(row -> !row.startsWith("label")).withName("Remove headers").withName("Remove headers")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(line -> line.split(","))
                .map(r -> new Tuple2<>(Long.parseLong(r[0]), 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();

        Collection<Tuple2<String, Integer>> res = new ArrayList<>();
        for (Tuple2<Long, Integer> r : tmpRes) {
            res.add(new Tuple2<>(r.getField0().toString(), r.getField1()));
        }
        return res;
    }


    private static class BenchConf {
        private final String inputPath;
        private final String workload;
        private final boolean shouldUseProjection;

        public BenchConf(String inputPath, boolean shouldUseProjection, String workload) {
            this.inputPath = inputPath;
            this.shouldUseProjection = shouldUseProjection;
            this.workload = workload;
        }

        public static Collection<BenchConf> generateBenchmarksFromDir(String benchDir, String workload) {
            File dir = new File(benchDir);
            File[] directoryListing = dir.listFiles();

            if (directoryListing == null) {
                throw new RuntimeException("Invalid dir: " + benchDir);
            }

            ArrayList<BenchConf> benchConfs = new ArrayList<>();
            for (File child : directoryListing) {
                String path = "file://" + child.toString();
                benchConfs.add(new BenchConf(path, false, workload));

                if (path.endsWith(".parquet")) {
                    benchConfs.add(new BenchConf(path, true, workload));
                }
            }

            benchConfs.sort((a, b) -> (b.shouldUseProjection ? 1 : 0) - (a.shouldUseProjection ? 1 : 0));

            return benchConfs;
        }
    }

    private static class BenchmarkResult {
        public BenchConf benchConf;
        public Experiment experiment;
        public long numRecords;
        public Collection<Tuple2<String, Integer>> results;

        public BenchmarkResult(BenchConf benchConf, Experiment experiment, long numRecords, Collection<Tuple2<String, Integer>> results) {
            this.benchConf = benchConf;
            this.experiment = experiment;
            this.numRecords = numRecords;
            this.results = results;
        }
    }
}
