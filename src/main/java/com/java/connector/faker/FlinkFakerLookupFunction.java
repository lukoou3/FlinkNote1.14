package com.java.connector.faker;

import net.datafaker.Faker;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.types.logical.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class FlinkFakerLookupFunction extends TableFunction<RowData> {

  private String[][] fieldExpressions;
  private Float[] fieldNullRates;
  private Integer[] fieldCollectionLengths;
  private LogicalType[] types;
  private int[][] keys;
  private List<Integer> keyIndeces;
  private Faker faker;
  private Random rand;

  public FlinkFakerLookupFunction(
      String[][] fieldExpressions,
      Float[] fieldNullRates,
      Integer[] fieldCollectionLengths,
      LogicalType[] types,
      int[][] keys) {
    this.fieldExpressions = fieldExpressions;
    this.fieldNullRates = fieldNullRates;
    this.fieldCollectionLengths = fieldCollectionLengths;
    this.types = types;

    keyIndeces = new ArrayList<>();
    for (int i = 0; i < keys.length; i++) {
      // 不支持嵌套的row类型, keys实际就是一维数组
      // we don't support nested rows for now, so this is actually one-dimensional
      keyIndeces.add(keys[i][0]);
    }

    this.keys = keys;
  }

  @Override
  public void open(FunctionContext context) throws Exception {
    super.open(context);
    faker = new Faker();
    rand = new Random();
  }

  public void eval(Object... keys) {
    GenericRowData row = new GenericRowData(fieldExpressions.length);
    int keyCount = 0;
    for (int i = 0; i < fieldExpressions.length; i++) {
      // 如果是关联的key, 直接赋值就行
      if (keyIndeces.contains(i)) {
        row.setField(i, keys[keyCount]);
        keyCount++;
      } else {
        float fieldNullRate = fieldNullRates[i];
        // 这里根据概率确定是否关联到数据, 生成的数据都是随机的
        if (rand.nextFloat() > fieldNullRate) {
          List<String> values = new ArrayList<>();
          for (int j = 0; j < fieldCollectionLengths[i]; j++) {
            for (int k = 0; k < fieldExpressions[i].length; k++) {
              // loop for multiple expressions of one field (like map, row fields)
              values.add(faker.expression(fieldExpressions[i][k]));
            }
          }
          // 把生成的string类型的数据转换成flink sql内部类型，设置row
          row.setField(
              i, FakerUtils.stringValueToType(values.toArray(new String[values.size()]), types[i]));
        } else {
          row.setField(i, null);
        }
      }
    }
    collect(row);
  }
}
