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

package org.apache.wayang.basic.operators;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.wayang.core.plan.wayangplan.UnarySource;
import org.apache.wayang.core.types.DataSetType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This source reads a parquet file.
 */
public class ParquetFileSource extends UnarySource<GenericRecord> {
    private final Logger logger = LogManager.getLogger(this.getClass());
    private final String inputUrl;
    private final Schema projection;

    public ParquetFileSource(String inputUrl) {
        this(inputUrl, null);
    }

    public ParquetFileSource(String inputUrl, Schema projection) {
        super(DataSetType.createDefault(GenericRecord.class));
        this.inputUrl = inputUrl;
        this.projection = projection;
    }

    public ParquetFileSource(ParquetFileSource that) {
        super(that);
        this.inputUrl = that.getInputUrl();
        this.projection = that.getProjection();
    }

    public String getInputUrl() {
        return this.inputUrl;
    }

    public Schema getProjection() {
        return this.projection;
    }
}
