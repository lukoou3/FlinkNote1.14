package com.java.connector.common;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class BatchIntervalSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private int batchSize;
    private long batchIntervalMs;
    private long minPauseBetweenFlushMs;
    private boolean keyedMode;
    transient private boolean closed;
    transient private ScheduledExecutorService scheduler;
    transient private ScheduledFuture<?> scheduledFuture;
    transient private ReentrantLock lock;
    transient private List<T> batch;
    transient private Map<Object, T> keyedBatch;
    transient private Exception flushException;
    transient private long lastFlushTs = 0L;
    transient private long writeCount = 0L;
    transient private long writeBytes = 0L;
    transient private Counter numBytesOutCounter;
    transient private Counter numRecordsOutCounter;

    public BatchIntervalSink(int batchSize, long batchIntervalMs, long minPauseBetweenFlushMs, boolean keyedMode) {
        this.batchSize = batchSize;
        this.batchIntervalMs = batchIntervalMs;
        this.minPauseBetweenFlushMs = minPauseBetweenFlushMs;
        this.keyedMode = keyedMode;
    }

    abstract void onInit(Configuration parameters) throws Exception;

    abstract void onFlush(Iterable<T> datas) throws Exception;

    abstract void onClose() throws Exception;

    abstract T valueTransform(T data);

    public Object getKey(T data) {
        throw new RuntimeException("keyedMode必须实现");
    }

    public T replaceValue(T newValue, T oldValue) {
        return newValue;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        onInit(parameters);
        numBytesOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumBytesOutCounter();
        numRecordsOutCounter = getRuntimeContext().getMetricGroup().getIOMetricGroup().getNumRecordsOutCounter();

        if (!keyedMode) {
            batch = new ArrayList<>();
        } else {
            keyedBatch = new LinkedHashMap<>();
        }
        lastFlushTs = 0L;
        if (batchIntervalMs != 0 && batchSize != 1) {
            scheduler = Executors.newScheduledThreadPool(1, new ExecutorThreadFactory("BatchIntervalSink"));
            scheduledFuture = scheduler.scheduleWithFixedDelay(() -> {
                if(System.currentTimeMillis() - lastFlushTs < minPauseBetweenFlushMs) {
                    return;
                }

                lock.lock();
                try {
                    flush();
                } catch (Exception e) {
                    flushException = e;
                } finally {
                    lock.unlock();
                }

            }, batchIntervalMs, batchIntervalMs, TimeUnit.MILLISECONDS);
        }
    }

    public final void checkFlushException(){
        if (flushException != null)
            throw new RuntimeException("flush failed.", flushException);
    }

    public final int currentBatchCount(){
        return  !keyedMode? batch.size() : keyedBatch.size();
    }

    @Override
    public final void invoke(T value, Context context) throws Exception {
        checkFlushException();
        lock.lock();
        try {
            if(!keyedMode){
                batch.add(valueTransform(value));
            }else{
                T newValue = valueTransform(value);
                Object key = getKey(newValue);
                T oldValue = keyedBatch.get(key);
                if(oldValue != null){
                    keyedBatch.put(key, replaceValue(newValue, oldValue));
                }else{
                    keyedBatch.put(key, newValue);
                }
            }

            if (batchSize > 0 && currentBatchCount() >= batchSize) {
                flush();
            }
        }finally {
            lock.unlock();
        }
    }

    public final void incNumBytesOut(long bytes) {
        writeBytes += bytes;
        numBytesOutCounter.inc(bytes);
    }

    public final void flush() throws Exception{
        checkFlushException();
        lastFlushTs = System.currentTimeMillis();
        if(currentBatchCount() <= 0){
            return;
        }
        lock.lock();
        try {
            int currentBatchCount = this.currentBatchCount();
            if(!keyedMode){
                onFlush(batch);
                writeCount += currentBatchCount;
                numRecordsOutCounter.inc(currentBatchCount);
                batch.clear();
            }else{
                onFlush(keyedBatch.values());
                writeCount += currentBatchCount;
                numRecordsOutCounter.inc(currentBatchCount);
                keyedBatch.clear();
            }
        }finally {
            lock.unlock();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        flush();
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

    }

    @Override
    public void close() throws Exception {
        if (!closed) {
            closed = true;

            if (this.scheduledFuture != null) {
                this.scheduledFuture.cancel(false);
                this.scheduler.shutdown();
            }

            // init中可能抛出异常
            if(lock != null){
                lock.lock();
                try {
                    if (currentBatchCount() > 0) {
                        flush();
                    }
                } catch (Exception e) {
                    flushException = e;
                } finally {
                    lock.unlock();
                }
            }

            onClose();
        }

        checkFlushException();
    }
}
