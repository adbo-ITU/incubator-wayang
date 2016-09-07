package org.qcri.rheem.java.operators;

import org.qcri.rheem.core.optimizer.OptimizationContext;
import org.qcri.rheem.core.plan.rheemplan.ExecutionOperator;
import org.qcri.rheem.core.platform.ChannelInstance;
import org.qcri.rheem.java.channels.JavaChannelInstance;
import org.qcri.rheem.java.channels.StreamChannel;
import org.qcri.rheem.java.execution.JavaExecutor;
import org.qcri.rheem.java.platform.JavaPlatform;

import java.util.Collection;
import java.util.stream.Stream;

/**
 * Execution operator for the Java platform.
 */
public interface JavaExecutionOperator extends ExecutionOperator {

    @Override
    default JavaPlatform getPlatform() {
        return JavaPlatform.getInstance();
    }

    /**
     * Evaluates this operator. Takes a set of Java {@link Stream}s according to the operator inputs and produces
     * a set of {@link Stream}s according to the operator outputs -- unless the operator is a sink, then it triggers
     * execution.
     * <p>In addition, this method should give feedback of what this instance was doing by wiring the
     * {@link org.qcri.rheem.core.platform.LazyChannelLineage} of input and ouput {@link ChannelInstance}s and
     * providing a {@link Collection} of executed {@link OptimizationContext.OperatorContext}s.</p>
     *
     * @param inputs          {@link ChannelInstance}s that satisfy the inputs of this operator
     * @param outputs         {@link ChannelInstance}s that collect the outputs of this operator
     * @param javaExecutor    that executes this instance
     * @param operatorContext optimization information for this instance
     * @return a {@link Collection} of what has been executed
     */
    Collection<OptimizationContext.OperatorContext> evaluate(ChannelInstance[] inputs,
                                                       ChannelInstance[] outputs,
                                                       JavaExecutor javaExecutor,
                                                       OptimizationContext.OperatorContext operatorContext);

    /**
     * Utility method to forward a {@link JavaChannelInstance} to another.
     * @param input that should be forwarded
     * @param output to that should be forwarded
     */
    static void forward(JavaChannelInstance input, JavaChannelInstance output) {
        // Do the forward.
        ((StreamChannel.Instance) output).accept(input.provideStream());

        // Manipulate the lineage.
        output.getLazyChannelLineage().copyRootFrom(input.getLazyChannelLineage());
    }

}
