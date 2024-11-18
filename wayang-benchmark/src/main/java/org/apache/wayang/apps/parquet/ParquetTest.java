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

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.basic.data.Tuple2;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Subject;
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
        if (args.length < 1) {
            System.err.println("Usage: ParquetTest <bench-dir-path> [filter]");
            System.exit(1);
        }

        String benchDir = args[0];
        String filter = args.length > 1 ? args[1] : null;
        Collection<Workload> workloads = Workload.generateBenchmarksFromDir(benchDir);

        for (Workload workload : workloads) {
            System.out.printf("%nWorkload %s with proj: %b%n", workload.inputPath, workload.shouldUseProjection);

            if (filter != null && !filter.isEmpty() && !workload.inputPath.contains(filter)) {
                System.out.println("Skipping due to filter..");
                continue;
            }

            BenchmarkResult result = run(workload);

            System.out.println("\nMeasurements:");
            for (Measurement m : result.experiment.getMeasurements()) {
                System.out.println("Measurement: " + m);
            }
            System.out.println();

            System.out.printf("Processed %d records. Results:\n", result.numRecords);
            result.results.forEach(res -> System.out.printf("%s\n", res.toString()));
        }
    }

    private static BenchmarkResult run(Workload workload) {
        Experiment experiment = new Experiment("parquet-bench-exp", new Subject("parquet-bench", "v0.1"));

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withExperiment(experiment)
                .withUdfJarOf(ParquetTest.class);

        AtomicLong numRecords = new AtomicLong();
        Collection<Tuple2<String, Integer>> results;

        if (workload.inputPath.endsWith(".parquet")) {
            results = runParquet(workload, planBuilder, numRecords);
        } else if (workload.inputPath.endsWith(".csv")) {
            results = runCsv(workload, planBuilder, numRecords);
        } else {
            throw new RuntimeException("Unknown file format: " + workload.inputPath);
        }

        return new BenchmarkResult(workload, experiment, numRecords.get(), results);
    }

    private static Collection<Tuple2<String, Integer>> runParquet(Workload workload, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        Schema projection = workload.shouldUseProjection
                ? SchemaBuilder.record("ParquetProjection")
                .fields()
                .optionalString("lo_shipmode")
                .endRecord()
                : null;

        return planBuilder
                .readParquet(workload.inputPath, projection).withName("Load parquet file")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(r -> new Tuple2<>(r.get("lo_shipmode").toString(), 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();
    }

    private static Collection<Tuple2<String, Integer>> runCsv(Workload workload, JavaPlanBuilder planBuilder, AtomicLong numRecords) {
        return planBuilder
                .readTextFile(workload.inputPath).withName("Load text file")
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

    private static class Workload {
        private final String inputPath;
        private final boolean shouldUseProjection;

        public Workload(String inputPath, boolean shouldUseProjection) {
            this.inputPath = inputPath;
            this.shouldUseProjection = shouldUseProjection;
        }

        public static Collection<Workload> generateBenchmarksFromDir(String benchDir) {
            File dir = new File(benchDir);
            File[] directoryListing = dir.listFiles();

            if (directoryListing == null) {
                throw new RuntimeException("Invalid dir: " + benchDir);
            }

            ArrayList<Workload> workloads = new ArrayList<>();
            for (File child : directoryListing) {
                String path = "file://" + child.toString();
                workloads.add(new Workload(path, false));

                if (path.endsWith(".parquet")) {
                    workloads.add(new Workload(path, true));
                }
            }

            workloads.sort((a, b) -> (b.shouldUseProjection ? 1 : 0) - (a.shouldUseProjection ? 1 : 0));

            return workloads;
        }
    }

    private static class BenchmarkResult {
        public Workload workload;
        public Experiment experiment;
        public long numRecords;
        public Collection<Tuple2<String, Integer>> results;

        public BenchmarkResult(Workload workload, Experiment experiment, long numRecords, Collection<Tuple2<String, Integer>> results) {
            this.workload = workload;
            this.experiment = experiment;
            this.numRecords = numRecords;
            this.results = results;
        }
    }
}
