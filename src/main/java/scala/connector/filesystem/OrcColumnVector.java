package scala.connector.filesystem;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.types.DataType;
import org.apache.hadoop.hive.ql.exec.vector.*;

import static org.apache.flink.table.types.logical.LogicalTypeRoot.DATE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITH_TIME_ZONE;

public class OrcColumnVector {
    private ColumnVector baseData;
    private LongColumnVector longData;
    private DoubleColumnVector doubleData;
    private BytesColumnVector bytesData;
    private DecimalColumnVector decimalData;
    private TimestampColumnVector timestampData;
    private final boolean isTimestamp;
    private final boolean isDate;

    private int batchSize;

    public OrcColumnVector(DataType type, ColumnVector vector) {
        //super(type);

        if (type.getLogicalType().getTypeRoot() == TIMESTAMP_WITH_TIME_ZONE) {
            isTimestamp = true;
        } else {
            isTimestamp = false;
        }

        if (type.getLogicalType().getTypeRoot() == DATE) {
            isDate = true;
        } else {
            isDate = false;
        }

        baseData = vector;
        if (vector instanceof LongColumnVector) {
            longData = (LongColumnVector) vector;
        } else if (vector instanceof DoubleColumnVector) {
            doubleData = (DoubleColumnVector) vector;
        } else if (vector instanceof BytesColumnVector) {
            bytesData = (BytesColumnVector) vector;
        } else if (vector instanceof DecimalColumnVector) {
            decimalData = (DecimalColumnVector) vector;
        } else if (vector instanceof TimestampColumnVector) {
            timestampData = (TimestampColumnVector) vector;
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /* A helper method to get the row index in a column. */
    private int getRowIndex(int rowId) {
        return baseData.isRepeating ? 0 : rowId;
    }

    public boolean isNullAt(int rowId) {
        return baseData.isNull[getRowIndex(rowId)];
    }

    public boolean getBoolean(int rowId) {
        return longData.vector[getRowIndex(rowId)] == 1;
    }

    public byte getByte(int rowId) {
        return (byte) longData.vector[getRowIndex(rowId)];
    }

    public short getShort(int rowId) {
        return (short) longData.vector[getRowIndex(rowId)];
    }

    public int getInt(int rowId) {
        int value = (int) longData.vector[getRowIndex(rowId)];
        if (isDate) {
            //return RebaseDateTime.rebaseJulianToGregorianDays(value);
            return value;
        } else {
            return value;
        }
    }

    public long getLong(int rowId) {
        int index = getRowIndex(rowId);
        if (isTimestamp) {
            return timestampData.getTime(index);
        } else {
            return longData.vector[index];
        }
    }

    public float getFloat(int rowId) {
        return (float) doubleData.vector[getRowIndex(rowId)];
    }

    public double getDouble(int rowId) {
        return doubleData.vector[getRowIndex(rowId)];
    }


    public StringData getString(int rowId) {
        if (isNullAt(rowId)) return null;
        int index = getRowIndex(rowId);
        BytesColumnVector col = bytesData;
        return StringData.fromBytes(col.vector[index], col.start[index], col.length[index]);
    }
}
