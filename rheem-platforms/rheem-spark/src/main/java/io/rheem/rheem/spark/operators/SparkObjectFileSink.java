package io.rheem.rheem.spark.operators;

import io.rheem.rheem.basic.channels.FileChannel;
import io.rheem.rheem.core.optimizer.OptimizationContext;
import io.rheem.rheem.core.plan.rheemplan.ExecutionOperator;
import io.rheem.rheem.core.plan.rheemplan.Operator;
import io.rheem.rheem.core.plan.rheemplan.UnarySink;
import io.rheem.rheem.core.platform.ChannelDescriptor;
import io.rheem.rheem.core.platform.ChannelInstance;
import io.rheem.rheem.core.platform.lineage.ExecutionLineageNode;
import io.rheem.rheem.core.types.DataSetType;
import io.rheem.rheem.core.util.Tuple;
import io.rheem.rheem.spark.channels.RddChannel;
import io.rheem.rheem.spark.execution.SparkExecutor;
import io.rheem.rheem.spark.platform.SparkPlatform;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * {@link Operator} for the {@link SparkPlatform} that creates a sequence file.
 *
 * @see SparkObjectFileSource
 */
public class SparkObjectFileSink<T> extends UnarySink<T> implements SparkExecutionOperator {

    private final String targetPath;

    public SparkObjectFileSink(DataSetType<T> type) {
        this(null, type);
    }

    public SparkObjectFileSink(String targetPath, DataSetType<T> type) {
        super(type);
        this.targetPath = targetPath;
    }

    @Override
    public Tuple<Collection<ExecutionLineageNode>, Collection<ChannelInstance>> evaluate(
            ChannelInstance[] inputs,
            ChannelInstance[] outputs,
            SparkExecutor sparkExecutor,
            OptimizationContext.OperatorContext operatorContext) {
        assert inputs.length == this.getNumInputs();
        assert outputs.length <= 1;

        final FileChannel.Instance output = (FileChannel.Instance) outputs[0];
        final String targetPath = output.addGivenOrTempPath(this.targetPath, sparkExecutor.getConfiguration());
        RddChannel.Instance input = (RddChannel.Instance) inputs[0];

        input.provideRdd()
                .coalesce(1) // TODO: Remove. This only hotfixes the issue that JavaObjectFileSource reads only a single file.
                .saveAsObjectFile(targetPath);
        LoggerFactory.getLogger(this.getClass()).info("Writing dataset to {}.", targetPath);

        return ExecutionOperator.modelEagerExecution(inputs, outputs, operatorContext);
    }

    @Override
    protected ExecutionOperator createCopy() {
        return new SparkObjectFileSink<>(targetPath, this.getType());
    }

    @Override
    public String getLoadProfileEstimatorConfigurationKey() {
        return "rheem.spark.objectfilesink.load";
    }

    @Override
    public List<ChannelDescriptor> getSupportedInputChannels(int index) {
        return Collections.singletonList(RddChannel.UNCACHED_DESCRIPTOR);
    }

    @Override
    public List<ChannelDescriptor> getSupportedOutputChannels(int index) {
        return Collections.singletonList(FileChannel.HDFS_OBJECT_FILE_DESCRIPTOR);
    }

    @Override
    public boolean containsAction() {
        return true;
    }

}