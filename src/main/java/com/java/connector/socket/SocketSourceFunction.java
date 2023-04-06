package com.java.connector.socket;

import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.RuntimeContextInitializationContextAdapters;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.Socket;
import java.nio.charset.StandardCharsets;

public class SocketSourceFunction<T> extends RichSourceFunction<T> {
    private static final Logger LOG = LoggerFactory.getLogger(SocketSourceFunction.class);
    private String host;
    private int port;
    private DeserializationSchema<T> deserializer;
    private boolean stop;

    public SocketSourceFunction(String host, int port, DeserializationSchema<T> deserializer) {
        this.host = host;
        this.port = port;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        deserializer.open(RuntimeContextInitializationContextAdapters.deserializationAdapter(getRuntimeContext()));
    }

    @Override
    public void run(SourceContext<T> ctx) throws Exception {
        try (Socket socket = new Socket(host, port)) {
            LOG.info("Connected to server socket " + host + ':' + port);
            LineIterator lines = IOUtils.lineIterator(socket.getInputStream(), "utf-8");
            while (!stop && lines.hasNext()) {
                String line = lines.next().trim();
                if(!line.isEmpty()){
                    T data = deserializer.deserialize(line.getBytes(StandardCharsets.UTF_8));
                    ctx.collect(data);
                }
            }
        }
    }

    @Override
    public void cancel() {
        stop = true;
    }
}
