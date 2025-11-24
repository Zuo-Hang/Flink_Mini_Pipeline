package com.streamforge.pipeline.functions;

import com.streamforge.pipeline.model.OrderEvent;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class DeduplicateFunction extends KeyedProcessFunction<String, OrderEvent, OrderEvent> {

    private final long ttlMillis;
    private transient ValueState<Boolean> seenState;

    public DeduplicateFunction(long ttlMillis) {
        this.ttlMillis = ttlMillis;
    }

    @Override
    public void open(Configuration parameters) {
        StateTtlConfig ttlConfig = StateTtlConfig.newBuilder(Time.milliseconds(ttlMillis))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>("dedupSeen", Types.BOOLEAN);
        descriptor.enableTimeToLive(ttlConfig);
        this.seenState = getRuntimeContext().getState(descriptor);
    }

    @Override
    public void processElement(OrderEvent event, Context ctx, Collector<OrderEvent> out) throws Exception {
        Boolean seen = seenState.value();
        if (Boolean.TRUE.equals(seen)) {
            return;
        }
        seenState.update(Boolean.TRUE);
        out.collect(event);
    }
}

