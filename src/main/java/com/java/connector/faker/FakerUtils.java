package com.java.connector.faker;

import org.apache.flink.table.data.*;
import org.apache.flink.table.types.logical.*;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class FakerUtils {

  public static final String FAKER_DATETIME_FORMAT = "EEE MMM dd HH:mm:ss zzz yyyy";

  private static DateTimeFormatter formatter =
      DateTimeFormatter.ofPattern(FAKER_DATETIME_FORMAT, new Locale("us"));

  private static final DateTimeFormatter FORMATTER =
          new DateTimeFormatterBuilder()
                  // Pattern was taken from java.sql.Timestamp#toString
                  //.appendPattern("uuuu-MM-dd HH:mm:ss")
                  .appendPattern("yyyy-MM-dd HH:mm:ss")
                  .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                  .toFormatter(Locale.US);

  static Object stringValueToType(String[] stringArray, LogicalType logicalType) {
    String value = stringArray.length > 0 ? stringArray[0] : "";

    switch (logicalType.getTypeRoot()) {
      case CHAR:
      case VARCHAR:
        return StringData.fromString(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      case DECIMAL:
        BigDecimal bd = new BigDecimal(value);
        return DecimalData.fromBigDecimal(bd, bd.precision(), bd.scale());
      case TINYINT:
        return Byte.parseByte(value);
      case SMALLINT:
        return Short.parseShort(value);
      case INTEGER:
        return Integer.parseInt(value);
      case BIGINT:
        return Long.parseLong(value);
        //return new BigInteger(value);
      case FLOAT:
        return Float.parseFloat(value);
      case DOUBLE:
        return Double.parseDouble(value);
        //      case DATE:
        //        break;
      case DATE:
        return (int)
                (Date.from(Instant.from(FORMATTER.withZone(ZoneId.systemDefault()).parse(value)))
                        .getTime()
                        / (86400 * 1000));
      case TIME_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITHOUT_TIME_ZONE:
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        return TimestampData.fromInstant(
                Instant.from(FORMATTER.withZone(ZoneId.systemDefault()).parse(value)));
        //LocalDateTime localDateTime = LocalDateTime.parse(value, formatter);
        //long ts = localDateTime.toEpochSecond(ZoneOffset.of("+8")) * 1000;
        //return TimestampData.fromEpochMillis(ts);
        // return TimestampData.fromInstant(Instant.from(formatter.parse(value)));
        //        break;
        //              case INTERVAL_YEAR_MONTH:
        //        break;
        //      case INTERVAL_DAY_TIME:
        //        break;
      case ARRAY:
        if(((ArrayType) logicalType).getElementType().getTypeRoot() == LogicalTypeRoot.ROW){
          int fieldSize = ((RowType)((ArrayType)logicalType).getElementType()).getFields().size();
          Object[] arrayElements = new Object[stringArray.length / fieldSize];
          for (int i = 0; i < arrayElements.length; i += 1){
            String[] valueArray = new String[fieldSize];
            for (int j = 0; j < fieldSize; j += 1){
              valueArray[j] = stringArray[i * fieldSize + j];
            }
            arrayElements[i] =
                    (stringValueToType(
                            valueArray, ((ArrayType) logicalType).getElementType()));
          }
          return new GenericArrayData(arrayElements);
        }else{
          Object[] arrayElements = new Object[stringArray.length];
          for (int i = 0; i < stringArray.length; i++)
            arrayElements[i] =
                    (stringValueToType(
                            new String[] {stringArray[i]}, ((ArrayType) logicalType).getElementType()));
          return new GenericArrayData(arrayElements);
        }
      case MULTISET:
        Map<Object, Integer> multisetMap = new HashMap<>();
        for (int i = 0; i < stringArray.length; i++) {
          Object element =
              stringValueToType(
                  new String[] {stringArray[i]}, ((MultisetType) logicalType).getElementType());
          Integer multiplicity =
              multisetMap.containsKey(element) ? (multisetMap.get(element) + 1) : 1;
          multisetMap.put(element, multiplicity);
        }
        return new GenericMapData(multisetMap);
      case MAP:
        Map<Object, Object> map = new HashMap<>();
        for (int i = 0; i < stringArray.length; i += 2) {
          Object key =
              stringValueToType(
                  new String[] {stringArray[i]}, ((MapType) logicalType).getKeyType());
          Object val =
              stringValueToType(
                  new String[] {stringArray[i + 1]}, ((MapType) logicalType).getValueType());
          map.put(key, val);
        }
        return new GenericMapData(map);
      case ROW:
        GenericRowData row = new GenericRowData(stringArray.length);
        for (int i = 0; i < ((RowType) logicalType).getFieldCount(); i++) {
          Object obj =
              stringValueToType(
                  new String[] {stringArray[i]}, ((RowType) logicalType).getTypeAt(i));
          row.setField(i, obj);
        }
        return row;
        //      case DISTINCT_TYPE:
        //        break;
        //      case STRUCTURED_TYPE:
        //        break;
        //      case NULL:
        //        break;
        //      case RAW:
        //        break;
        //      case SYMBOL:
        //        break;
        //      case UNRESOLVED:
        //        break;
      default:
        throw new RuntimeException("Unsupported Data Type");
    }
  }
}
