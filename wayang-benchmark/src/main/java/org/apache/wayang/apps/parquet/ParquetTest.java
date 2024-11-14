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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.net.URISyntaxException;

public class ParquetTest {
    public static void main(String[] args) throws IOException, URISyntaxException {
        if (args.length != 1) {
            System.err.println("Usage: ParquetTest <input-path>");
            System.exit(1);
        }

        String pathStr = args[0];

        Path path = new Path(pathStr);
        InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());

        System.out.println("Læser fraaaaaaaaaaa!!!!!!: " + inputFile.toString());

        try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
            System.out.println("REKORD " + reader.read().toString());
        }
    }
}
