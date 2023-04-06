package com.java.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;

public class SocketDynamicTableSource implements ScanTableSource {
    private String host;
    private int port;
    private DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
    private DataType producedDataType;

    public SocketDynamicTableSource(String host, int port, DecodingFormat<DeserializationSchema<RowData>> decodingFormat, DataType producedDataType) {
        this.host = host;
        this.port = port;
        this.decodingFormat = decodingFormat;
        this.producedDataType = producedDataType;
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.insertOnly();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializer = decodingFormat.createRuntimeDecoder(runtimeProviderContext, producedDataType);
        SocketSourceFunction<RowData> func = new SocketSourceFunction<>(host, port, deserializer);
        return SourceFunctionProvider.of(func, false);
    }

    @Override
    public DynamicTableSource copy() {
        return new SocketDynamicTableSource(host, port, decodingFormat, producedDataType);
    }

    @Override
    public String asSummaryString() {
        return null;
    }
}
