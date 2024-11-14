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

package org.apache.wayang.java.operators;

import org.apache.wayang.basic.operators.ParquetFileSource;
import org.apache.wayang.core.optimizer.OptimizationContext;
import org.apache.wayang.core.platform.ChannelDescriptor;
import org.apache.wayang.core.platform.ChannelInstance;
import org.apache.wayang.core.platform.lineage.ExecutionLineageNode;
import org.apache.wayang.core.util.Tuple;
import org.apache.wayang.java.channels.StreamChannel;
import org.apache.wayang.java.execution.JavaExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.io.InputFile;

import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

public class JavaParquetFileSource extends ParquetFileSource implements JavaExecutionOperator {
    private static final Logger logger = LoggerFactory.getLogger(JavaParquetFileSource.class);

    public JavaParquetFileSource(String inputUrl) {
        super(inputUrl);
    }

    public JavaParquetFileSource(ParquetFileSource that) {
        super(that);
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            JavaExecutor javaExecutor,
            OptimizationContext.OperatorContext operatorContext) {

        Path path = new Path(this.getInputUrl());

        try {
            InputFile inputFile = HadoopInputFile.fromPath(path, new Configuration());

            try (ParquetReader<GenericRecord> reader = AvroParquetReader.<GenericRecord>builder(inputFile).build()) {
                // TODO: lazy stream here rather than all-at-once in memory
                ArrayList<GenericRecord> records = new ArrayList<>();

                while (true) {
                    GenericRecord record = reader.read();
                    if (record == null) { break; }
                    records.add(record);
                }

                Stream<GenericRecord> stream = records.stream();
                ((org.apache.wayang.java.channels.StreamChannel.Instance) outputs[0]).accept(stream);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }


        ExecutionLineageNode mainLineageNode = new ExecutionLineageNode(operatorContext);

        outputs[0].getLineage().addPredecessor(mainLineageNode);

        return mainLineageNode.collectAndMark();
    }

    @Override
    public JavaParquetFileSource copy() {
        return new JavaParquetFileSource(this.getInputUrl());
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        throw new UnsupportedOperationException(String.format("%s does not have input channels.", this));
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(StreamChannel.DESCRIPTOR);
    }

}
