package com.java.connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

public class SocketDynamicTableFactory implements DynamicTableSourceFactory {
    public static final ConfigOption<String> HOSTNAME = ConfigOptions.key("hostname").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().noDefaultValue();

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig config = helper.getOptions();

        // 先调这个，helper.validate()不会报format的属性不存在
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat  = helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);

        helper.validate();

        /**
         * 不过没事，KafkaDynamicTableFactory也调用的这个，内部调用了新的没废弃的方法
         * deprecated: org.apache.flink.table.catalog.ResolvedCatalogBaseTable.getSchema
         */
        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

        String host = config.get(HOSTNAME);
        int port = config.get(PORT);

        return  new SocketDynamicTableSource(host, port, decodingFormat, producedDataType);
    }

    @Override
    public String factoryIdentifier() {
        return "java-mysocket";
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        options.add(HOSTNAME);
        options.add(PORT);
        options.add(FactoryUtil.FORMAT); // use pre-defined option for format
        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet();
        return options;
    }
}
