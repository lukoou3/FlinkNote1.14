package scala.connector.faker;

import static scala.connector.faker.FlinkFakerTableSourceFactory.UNLIMITED_ROWS;

import java.util.Arrays;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.*;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;

public class FlinkFakerTableSource implements ScanTableSource, LookupTableSource {

  private String[][] fieldExpressions;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private TableSchema schema;
  private final LogicalType[] types;
  private long rowsPerSecond;
  private long numberOfRows;
  private long sleepPerRow;

  public FlinkFakerTableSource(
      String[][] fieldExpressions,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      TableSchema schema,
      long rowsPerSecond,
      long numberOfRows,
      long sleepPerRow) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.schema = schema;
    // LogicalType
    types =
        Arrays.stream(schema.getFieldDataTypes())
            .map(DataType::getLogicalType)
            .toArray(LogicalType[]::new);
    this.rowsPerSecond = rowsPerSecond;
    this.numberOfRows = numberOfRows;
    this.sleepPerRow = sleepPerRow;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.insertOnly();
  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(final ScanContext scanContext) {
    //System.out.println("getScanRuntimeProvider");
    //boolean isBounded = numberOfRows != UNLIMITED_ROWS;
    boolean isBounded = false;
    return SourceFunctionProvider.of(
        new FlinkFakerSourceFunction(
            fieldExpressions,
            fieldNullRates,
            fieldCollectionLengths,
            types,
            rowsPerSecond,
            numberOfRows,
                sleepPerRow),
        isBounded);
  }

  @Override
  public DynamicTableSource copy() {
    return new FlinkFakerTableSource(
        fieldExpressions,
        fieldNullRates,
        fieldCollectionLengths,
        schema,
        rowsPerSecond,
        numberOfRows, sleepPerRow);
  }

  @Override
  public String asSummaryString() {
    return "FlinkFakerSource";
  }

  @Override
  public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
    /**
     * context.getKeys()?????? int[][]???????????????key?????????, [?????????????????????, row?????????????????????????????????]
     * ???????????????????????????row?????????????????????, ????????????keys????????????????????????
     *    ?????????????????? a int, b int, c int, d int
     *    ??????????????????a, ???keys = [[0]]
     *    ??????????????????a???b, ???keys = [[0], [1]]
     *
     *    ?????????????????? i INT, s STRING, r ROW < i2 INT, s2 STRING >
     *    ??????????????????i???s2, ???keys = [[0], [2, 1]]
     *
     */
    return TableFunctionProvider.of(
        new FlinkFakerLookupFunction(
            fieldExpressions, fieldNullRates, fieldCollectionLengths, types, context.getKeys()));
  }
}
