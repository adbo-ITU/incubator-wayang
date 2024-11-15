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

import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.api.JavaPlanBuilder;
import org.apache.wayang.core.api.WayangContext;
import org.apache.wayang.java.Java;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Collection;

public class ParquetTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 1) {
            System.err.println("Usage: ParquetTest <input-path>");
            System.exit(1);
        }

        String pathStr = args[0];

        WayangContext wayangContext = new WayangContext();
        wayangContext.register(Java.basicPlugin());
        JavaPlanBuilder planBuilder = new JavaPlanBuilder(wayangContext)
                .withJobName("ParquetVroom")
                .withUdfJarOf(ParquetTest.class);

        Collection<GenericRecord> records = planBuilder
                .readParquet(pathStr).withName("Load file")
                .collect();

        System.out.printf("Found %d records:\n", records.size());
        records.forEach(record -> System.out.printf("%s\n", record.toString()));
    }
}
