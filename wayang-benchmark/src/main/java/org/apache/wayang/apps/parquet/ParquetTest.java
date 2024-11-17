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

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

public class ParquetTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 2 || (!args[1].equals("yes") && !args[1].equals("no"))) {
            System.err.println("Usage: ParquetTest <input-path> <yes|no for projection>");
            System.exit(1);
        }

        String pathStr = args[0];
        boolean shouldUseProjection = args[1].equals("yes");

        BenchmarkResult result = run(new Workload(pathStr, shouldUseProjection));

        System.out.println("\nMeasurements:");
        for (Measurement m : result.experiment.getMeasurements()) {
            System.out.println("Measurement: " + m);
        }
        System.out.println();

        System.out.printf("Processed %d records. Results:\n", result.numRecords);
        result.results.forEach(res -> System.out.printf("%s\n", res.toString()));
    }

    private static BenchmarkResult run(Workload workload) {
        Experiment experiment = new Experiment("parquet-bench-exp", new Subject("parquet-bench", "v0.1"));

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());

        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withExperiment(experiment)
                .withUdfJarOf(ParquetTest.class);

        Schema projection = workload.shouldUseProjection
            ? SchemaBuilder.record("ParquetProjection")
                .fields()
                .optionalString("lo_shipmode")
                .endRecord()
            : null;

        AtomicLong numRecords = new AtomicLong();
        Collection<Tuple2<String, Integer>> results = planBuilder
                .readParquet(workload.inputPath, projection).withName("Load file")
                .map((r) -> { numRecords.getAndIncrement(); return r; })
                .map(r -> new Tuple2<>(r.get("lo_shipmode").toString(), 1)).withName("Extract, add counter")
                .reduceByKey(
                        Tuple2::getField0,
                        (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1())
                )
                .withName("Add counters")
                .collect();

        return new BenchmarkResult(workload, experiment, numRecords.get(), results);
    }

    private static class Workload {
        private final String inputPath;
        private final boolean shouldUseProjection;

        public Workload(String inputPath, boolean shouldUseProjection) {
            this.inputPath = inputPath;
            this.shouldUseProjection = shouldUseProjection;
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
