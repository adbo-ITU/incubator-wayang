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
import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.commons.util.profiledb.model.Experiment;
import org.apache.wayang.commons.util.profiledb.model.Measurement;
import org.apache.wayang.commons.util.profiledb.model.Subject;
import org.apache.wayang.commons.util.profiledb.model.measurement.TimeMeasurement;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

public class ParquetTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 2 || (!args[1].equals("yes") && !args[1].equals("no"))) {
            System.err.println("Usage: ParquetTest <input-path> <yes|no for projection>");
            System.exit(1);
        }

        String pathStr = args[0];

        System.out.printf("Starting benchmark for reading '%s'%n", pathStr);
        long startTime = System.currentTimeMillis();

        Experiment experiment = new Experiment("parquet-bench-exp", new Subject("parquet-bench", "v0.1"));

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withExperiment(experiment)
                .withUdfJarOf(ParquetTest.class);

        Schema projection = args[1].equals("yes")
            ? SchemaBuilder.record("ParquetProjection")
                .fields()
                .optionalInt("lo_orderkey")
                .optionalInt("lo_linenumber")
                .optionalString("lo_orderpriority")
                .optionalString("lo_shippriority")
                .optionalString("p_name")
                .endRecord()
            : null;

        Collection<GenericRecord> records = planBuilder
                .readParquet(pathStr, projection).withName("Load file")
                .collect();

        long stopTime = System.currentTimeMillis();
        long elapsedTime = stopTime - startTime;
        System.out.printf("Total time %d ms%n", elapsedTime);

        System.out.println("\nMeasurements:");
        for (Measurement m : experiment.getMeasurements()) {
            if (m instanceof TimeMeasurement) {
                System.out.println("Time measurement: " + m);
            }
        }
        System.out.println();

        System.out.printf("Found %d records:\n", records.size());
        records.stream().limit(10).forEach(record -> System.out.printf("%s\n", record.toString()));
    }
}
