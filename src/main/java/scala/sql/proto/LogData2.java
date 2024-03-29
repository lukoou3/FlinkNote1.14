// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: log_data2.proto

package scala.sql.proto;

/**
 * Protobuf type {@code LogData2}
 */
public final class LogData2 extends
    com.google.protobuf.GeneratedMessageV3 implements
    // @@protoc_insertion_point(message_implements:LogData2)
    LogData2OrBuilder {
private static final long serialVersionUID = 0L;
  // Use LogData2.newBuilder() to construct.
  private LogData2(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
    super(builder);
  }
  private LogData2() {
    dt_ = "";
    bs_ = "";
    reportTime_ = "";
    visitTime_ = "";
  }

  @Override
  @SuppressWarnings({"unused"})
  protected Object newInstance(
      UnusedPrivateParameter unused) {
    return new LogData2();
  }

  public static final com.google.protobuf.Descriptors.Descriptor
      getDescriptor() {
    return LogData2Proto.internal_static_LogData2_descriptor;
  }

  @Override
  protected FieldAccessorTable
      internalGetFieldAccessorTable() {
    return LogData2Proto.internal_static_LogData2_fieldAccessorTable
        .ensureFieldAccessorsInitialized(
            LogData2.class, Builder.class);
  }

  public static final int DT_FIELD_NUMBER = 1;
  @SuppressWarnings("serial")
  private volatile Object dt_ = "";
  /**
   * <code>string dt = 1;</code>
   * @return The dt.
   */
  @Override
  public String getDt() {
    Object ref = dt_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      dt_ = s;
      return s;
    }
  }
  /**
   * <code>string dt = 1;</code>
   * @return The bytes for dt.
   */
  @Override
  public com.google.protobuf.ByteString
      getDtBytes() {
    Object ref = dt_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      dt_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int BS_FIELD_NUMBER = 2;
  @SuppressWarnings("serial")
  private volatile Object bs_ = "";
  /**
   * <pre>
   * Unique ID number for this person.
   * </pre>
   *
   * <code>string bs = 2;</code>
   * @return The bs.
   */
  @Override
  public String getBs() {
    Object ref = bs_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      bs_ = s;
      return s;
    }
  }
  /**
   * <pre>
   * Unique ID number for this person.
   * </pre>
   *
   * <code>string bs = 2;</code>
   * @return The bytes for bs.
   */
  @Override
  public com.google.protobuf.ByteString
      getBsBytes() {
    Object ref = bs_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      bs_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int REPORT_TIME_FIELD_NUMBER = 3;
  @SuppressWarnings("serial")
  private volatile Object reportTime_ = "";
  /**
   * <code>string report_time = 3;</code>
   * @return The reportTime.
   */
  @Override
  public String getReportTime() {
    Object ref = reportTime_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      reportTime_ = s;
      return s;
    }
  }
  /**
   * <code>string report_time = 3;</code>
   * @return The bytes for reportTime.
   */
  @Override
  public com.google.protobuf.ByteString
      getReportTimeBytes() {
    Object ref = reportTime_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      reportTime_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  public static final int ITEM_ID_FIELD_NUMBER = 9;
  private long itemId_ = 0L;
  /**
   * <code>int64 item_id = 9;</code>
   * @return The itemId.
   */
  @Override
  public long getItemId() {
    return itemId_;
  }

  public static final int ITEM_TYPE_FIELD_NUMBER = 10;
  private int itemType_ = 0;
  /**
   * <code>int32 item_type = 10;</code>
   * @return The itemType.
   */
  @Override
  public int getItemType() {
    return itemType_;
  }

  public static final int VISIT_TIME_FIELD_NUMBER = 11;
  @SuppressWarnings("serial")
  private volatile Object visitTime_ = "";
  /**
   * <code>string visit_time = 11;</code>
   * @return The visitTime.
   */
  @Override
  public String getVisitTime() {
    Object ref = visitTime_;
    if (ref instanceof String) {
      return (String) ref;
    } else {
      com.google.protobuf.ByteString bs = 
          (com.google.protobuf.ByteString) ref;
      String s = bs.toStringUtf8();
      visitTime_ = s;
      return s;
    }
  }
  /**
   * <code>string visit_time = 11;</code>
   * @return The bytes for visitTime.
   */
  @Override
  public com.google.protobuf.ByteString
      getVisitTimeBytes() {
    Object ref = visitTime_;
    if (ref instanceof String) {
      com.google.protobuf.ByteString b = 
          com.google.protobuf.ByteString.copyFromUtf8(
              (String) ref);
      visitTime_ = b;
      return b;
    } else {
      return (com.google.protobuf.ByteString) ref;
    }
  }

  private byte memoizedIsInitialized = -1;
  @Override
  public final boolean isInitialized() {
    byte isInitialized = memoizedIsInitialized;
    if (isInitialized == 1) return true;
    if (isInitialized == 0) return false;

    memoizedIsInitialized = 1;
    return true;
  }

  @Override
  public void writeTo(com.google.protobuf.CodedOutputStream output)
                      throws java.io.IOException {
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(dt_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 1, dt_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(bs_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 2, bs_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(reportTime_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 3, reportTime_);
    }
    if (itemId_ != 0L) {
      output.writeInt64(9, itemId_);
    }
    if (itemType_ != 0) {
      output.writeInt32(10, itemType_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(visitTime_)) {
      com.google.protobuf.GeneratedMessageV3.writeString(output, 11, visitTime_);
    }
    getUnknownFields().writeTo(output);
  }

  @Override
  public int getSerializedSize() {
    int size = memoizedSize;
    if (size != -1) return size;

    size = 0;
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(dt_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, dt_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(bs_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(2, bs_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(reportTime_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(3, reportTime_);
    }
    if (itemId_ != 0L) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt64Size(9, itemId_);
    }
    if (itemType_ != 0) {
      size += com.google.protobuf.CodedOutputStream
        .computeInt32Size(10, itemType_);
    }
    if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(visitTime_)) {
      size += com.google.protobuf.GeneratedMessageV3.computeStringSize(11, visitTime_);
    }
    size += getUnknownFields().getSerializedSize();
    memoizedSize = size;
    return size;
  }

  @Override
  public boolean equals(final Object obj) {
    if (obj == this) {
     return true;
    }
    if (!(obj instanceof LogData2)) {
      return super.equals(obj);
    }
    LogData2 other = (LogData2) obj;

    if (!getDt()
        .equals(other.getDt())) return false;
    if (!getBs()
        .equals(other.getBs())) return false;
    if (!getReportTime()
        .equals(other.getReportTime())) return false;
    if (getItemId()
        != other.getItemId()) return false;
    if (getItemType()
        != other.getItemType()) return false;
    if (!getVisitTime()
        .equals(other.getVisitTime())) return false;
    if (!getUnknownFields().equals(other.getUnknownFields())) return false;
    return true;
  }

  @Override
  public int hashCode() {
    if (memoizedHashCode != 0) {
      return memoizedHashCode;
    }
    int hash = 41;
    hash = (19 * hash) + getDescriptor().hashCode();
    hash = (37 * hash) + DT_FIELD_NUMBER;
    hash = (53 * hash) + getDt().hashCode();
    hash = (37 * hash) + BS_FIELD_NUMBER;
    hash = (53 * hash) + getBs().hashCode();
    hash = (37 * hash) + REPORT_TIME_FIELD_NUMBER;
    hash = (53 * hash) + getReportTime().hashCode();
    hash = (37 * hash) + ITEM_ID_FIELD_NUMBER;
    hash = (53 * hash) + com.google.protobuf.Internal.hashLong(
        getItemId());
    hash = (37 * hash) + ITEM_TYPE_FIELD_NUMBER;
    hash = (53 * hash) + getItemType();
    hash = (37 * hash) + VISIT_TIME_FIELD_NUMBER;
    hash = (53 * hash) + getVisitTime().hashCode();
    hash = (29 * hash) + getUnknownFields().hashCode();
    memoizedHashCode = hash;
    return hash;
  }

  public static LogData2 parseFrom(
      java.nio.ByteBuffer data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static LogData2 parseFrom(
      java.nio.ByteBuffer data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static LogData2 parseFrom(
      com.google.protobuf.ByteString data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static LogData2 parseFrom(
      com.google.protobuf.ByteString data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static LogData2 parseFrom(byte[] data)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data);
  }
  public static LogData2 parseFrom(
      byte[] data,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws com.google.protobuf.InvalidProtocolBufferException {
    return PARSER.parseFrom(data, extensionRegistry);
  }
  public static LogData2 parseFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static LogData2 parseFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }
  public static LogData2 parseDelimitedFrom(java.io.InputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input);
  }
  public static LogData2 parseDelimitedFrom(
      java.io.InputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseDelimitedWithIOException(PARSER, input, extensionRegistry);
  }
  public static LogData2 parseFrom(
      com.google.protobuf.CodedInputStream input)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input);
  }
  public static LogData2 parseFrom(
      com.google.protobuf.CodedInputStream input,
      com.google.protobuf.ExtensionRegistryLite extensionRegistry)
      throws java.io.IOException {
    return com.google.protobuf.GeneratedMessageV3
        .parseWithIOException(PARSER, input, extensionRegistry);
  }

  @Override
  public Builder newBuilderForType() { return newBuilder(); }
  public static Builder newBuilder() {
    return DEFAULT_INSTANCE.toBuilder();
  }
  public static Builder newBuilder(LogData2 prototype) {
    return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
  }
  @Override
  public Builder toBuilder() {
    return this == DEFAULT_INSTANCE
        ? new Builder() : new Builder().mergeFrom(this);
  }

  @Override
  protected Builder newBuilderForType(
      BuilderParent parent) {
    Builder builder = new Builder(parent);
    return builder;
  }
  /**
   * Protobuf type {@code LogData2}
   */
  public static final class Builder extends
      com.google.protobuf.GeneratedMessageV3.Builder<Builder> implements
      // @@protoc_insertion_point(builder_implements:LogData2)
      LogData2OrBuilder {
    public static final com.google.protobuf.Descriptors.Descriptor
        getDescriptor() {
      return LogData2Proto.internal_static_LogData2_descriptor;
    }

    @Override
    protected FieldAccessorTable
        internalGetFieldAccessorTable() {
      return LogData2Proto.internal_static_LogData2_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              LogData2.class, Builder.class);
    }

    // Construct using scala.sql.proto.LogData2.newBuilder()
    private Builder() {

    }

    private Builder(
        BuilderParent parent) {
      super(parent);

    }
    @Override
    public Builder clear() {
      super.clear();
      bitField0_ = 0;
      dt_ = "";
      bs_ = "";
      reportTime_ = "";
      itemId_ = 0L;
      itemType_ = 0;
      visitTime_ = "";
      return this;
    }

    @Override
    public com.google.protobuf.Descriptors.Descriptor
        getDescriptorForType() {
      return LogData2Proto.internal_static_LogData2_descriptor;
    }

    @Override
    public LogData2 getDefaultInstanceForType() {
      return LogData2.getDefaultInstance();
    }

    @Override
    public LogData2 build() {
      LogData2 result = buildPartial();
      if (!result.isInitialized()) {
        throw newUninitializedMessageException(result);
      }
      return result;
    }

    @Override
    public LogData2 buildPartial() {
      LogData2 result = new LogData2(this);
      if (bitField0_ != 0) { buildPartial0(result); }
      onBuilt();
      return result;
    }

    private void buildPartial0(LogData2 result) {
      int from_bitField0_ = bitField0_;
      if (((from_bitField0_ & 0x00000001) != 0)) {
        result.dt_ = dt_;
      }
      if (((from_bitField0_ & 0x00000002) != 0)) {
        result.bs_ = bs_;
      }
      if (((from_bitField0_ & 0x00000004) != 0)) {
        result.reportTime_ = reportTime_;
      }
      if (((from_bitField0_ & 0x00000008) != 0)) {
        result.itemId_ = itemId_;
      }
      if (((from_bitField0_ & 0x00000010) != 0)) {
        result.itemType_ = itemType_;
      }
      if (((from_bitField0_ & 0x00000020) != 0)) {
        result.visitTime_ = visitTime_;
      }
    }

    @Override
    public Builder mergeFrom(com.google.protobuf.Message other) {
      if (other instanceof LogData2) {
        return mergeFrom((LogData2)other);
      } else {
        super.mergeFrom(other);
        return this;
      }
    }

    public Builder mergeFrom(LogData2 other) {
      if (other == LogData2.getDefaultInstance()) return this;
      if (!other.getDt().isEmpty()) {
        dt_ = other.dt_;
        bitField0_ |= 0x00000001;
        onChanged();
      }
      if (!other.getBs().isEmpty()) {
        bs_ = other.bs_;
        bitField0_ |= 0x00000002;
        onChanged();
      }
      if (!other.getReportTime().isEmpty()) {
        reportTime_ = other.reportTime_;
        bitField0_ |= 0x00000004;
        onChanged();
      }
      if (other.getItemId() != 0L) {
        setItemId(other.getItemId());
      }
      if (other.getItemType() != 0) {
        setItemType(other.getItemType());
      }
      if (!other.getVisitTime().isEmpty()) {
        visitTime_ = other.visitTime_;
        bitField0_ |= 0x00000020;
        onChanged();
      }
      this.mergeUnknownFields(other.getUnknownFields());
      onChanged();
      return this;
    }

    @Override
    public final boolean isInitialized() {
      return true;
    }

    @Override
    public Builder mergeFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      if (extensionRegistry == null) {
        throw new NullPointerException();
      }
      try {
        boolean done = false;
        while (!done) {
          int tag = input.readTag();
          switch (tag) {
            case 0:
              done = true;
              break;
            case 10: {
              dt_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000001;
              break;
            } // case 10
            case 18: {
              bs_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000002;
              break;
            } // case 18
            case 26: {
              reportTime_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000004;
              break;
            } // case 26
            case 72: {
              itemId_ = input.readInt64();
              bitField0_ |= 0x00000008;
              break;
            } // case 72
            case 80: {
              itemType_ = input.readInt32();
              bitField0_ |= 0x00000010;
              break;
            } // case 80
            case 90: {
              visitTime_ = input.readStringRequireUtf8();
              bitField0_ |= 0x00000020;
              break;
            } // case 90
            default: {
              if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                done = true; // was an endgroup tag
              }
              break;
            } // default:
          } // switch (tag)
        } // while (!done)
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.unwrapIOException();
      } finally {
        onChanged();
      } // finally
      return this;
    }
    private int bitField0_;

    private Object dt_ = "";
    /**
     * <code>string dt = 1;</code>
     * @return The dt.
     */
    public String getDt() {
      Object ref = dt_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        dt_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string dt = 1;</code>
     * @return The bytes for dt.
     */
    public com.google.protobuf.ByteString
        getDtBytes() {
      Object ref = dt_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        dt_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string dt = 1;</code>
     * @param value The dt to set.
     * @return This builder for chaining.
     */
    public Builder setDt(
        String value) {
      if (value == null) { throw new NullPointerException(); }
      dt_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }
    /**
     * <code>string dt = 1;</code>
     * @return This builder for chaining.
     */
    public Builder clearDt() {
      dt_ = getDefaultInstance().getDt();
      bitField0_ = (bitField0_ & ~0x00000001);
      onChanged();
      return this;
    }
    /**
     * <code>string dt = 1;</code>
     * @param value The bytes for dt to set.
     * @return This builder for chaining.
     */
    public Builder setDtBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      dt_ = value;
      bitField0_ |= 0x00000001;
      onChanged();
      return this;
    }

    private Object bs_ = "";
    /**
     * <pre>
     * Unique ID number for this person.
     * </pre>
     *
     * <code>string bs = 2;</code>
     * @return The bs.
     */
    public String getBs() {
      Object ref = bs_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        bs_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <pre>
     * Unique ID number for this person.
     * </pre>
     *
     * <code>string bs = 2;</code>
     * @return The bytes for bs.
     */
    public com.google.protobuf.ByteString
        getBsBytes() {
      Object ref = bs_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        bs_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <pre>
     * Unique ID number for this person.
     * </pre>
     *
     * <code>string bs = 2;</code>
     * @param value The bs to set.
     * @return This builder for chaining.
     */
    public Builder setBs(
        String value) {
      if (value == null) { throw new NullPointerException(); }
      bs_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Unique ID number for this person.
     * </pre>
     *
     * <code>string bs = 2;</code>
     * @return This builder for chaining.
     */
    public Builder clearBs() {
      bs_ = getDefaultInstance().getBs();
      bitField0_ = (bitField0_ & ~0x00000002);
      onChanged();
      return this;
    }
    /**
     * <pre>
     * Unique ID number for this person.
     * </pre>
     *
     * <code>string bs = 2;</code>
     * @param value The bytes for bs to set.
     * @return This builder for chaining.
     */
    public Builder setBsBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      bs_ = value;
      bitField0_ |= 0x00000002;
      onChanged();
      return this;
    }

    private Object reportTime_ = "";
    /**
     * <code>string report_time = 3;</code>
     * @return The reportTime.
     */
    public String getReportTime() {
      Object ref = reportTime_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        reportTime_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string report_time = 3;</code>
     * @return The bytes for reportTime.
     */
    public com.google.protobuf.ByteString
        getReportTimeBytes() {
      Object ref = reportTime_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        reportTime_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string report_time = 3;</code>
     * @param value The reportTime to set.
     * @return This builder for chaining.
     */
    public Builder setReportTime(
        String value) {
      if (value == null) { throw new NullPointerException(); }
      reportTime_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }
    /**
     * <code>string report_time = 3;</code>
     * @return This builder for chaining.
     */
    public Builder clearReportTime() {
      reportTime_ = getDefaultInstance().getReportTime();
      bitField0_ = (bitField0_ & ~0x00000004);
      onChanged();
      return this;
    }
    /**
     * <code>string report_time = 3;</code>
     * @param value The bytes for reportTime to set.
     * @return This builder for chaining.
     */
    public Builder setReportTimeBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      reportTime_ = value;
      bitField0_ |= 0x00000004;
      onChanged();
      return this;
    }

    private long itemId_ ;
    /**
     * <code>int64 item_id = 9;</code>
     * @return The itemId.
     */
    @Override
    public long getItemId() {
      return itemId_;
    }
    /**
     * <code>int64 item_id = 9;</code>
     * @param value The itemId to set.
     * @return This builder for chaining.
     */
    public Builder setItemId(long value) {

      itemId_ = value;
      bitField0_ |= 0x00000008;
      onChanged();
      return this;
    }
    /**
     * <code>int64 item_id = 9;</code>
     * @return This builder for chaining.
     */
    public Builder clearItemId() {
      bitField0_ = (bitField0_ & ~0x00000008);
      itemId_ = 0L;
      onChanged();
      return this;
    }

    private int itemType_ ;
    /**
     * <code>int32 item_type = 10;</code>
     * @return The itemType.
     */
    @Override
    public int getItemType() {
      return itemType_;
    }
    /**
     * <code>int32 item_type = 10;</code>
     * @param value The itemType to set.
     * @return This builder for chaining.
     */
    public Builder setItemType(int value) {

      itemType_ = value;
      bitField0_ |= 0x00000010;
      onChanged();
      return this;
    }
    /**
     * <code>int32 item_type = 10;</code>
     * @return This builder for chaining.
     */
    public Builder clearItemType() {
      bitField0_ = (bitField0_ & ~0x00000010);
      itemType_ = 0;
      onChanged();
      return this;
    }

    private Object visitTime_ = "";
    /**
     * <code>string visit_time = 11;</code>
     * @return The visitTime.
     */
    public String getVisitTime() {
      Object ref = visitTime_;
      if (!(ref instanceof String)) {
        com.google.protobuf.ByteString bs =
            (com.google.protobuf.ByteString) ref;
        String s = bs.toStringUtf8();
        visitTime_ = s;
        return s;
      } else {
        return (String) ref;
      }
    }
    /**
     * <code>string visit_time = 11;</code>
     * @return The bytes for visitTime.
     */
    public com.google.protobuf.ByteString
        getVisitTimeBytes() {
      Object ref = visitTime_;
      if (ref instanceof String) {
        com.google.protobuf.ByteString b = 
            com.google.protobuf.ByteString.copyFromUtf8(
                (String) ref);
        visitTime_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }
    /**
     * <code>string visit_time = 11;</code>
     * @param value The visitTime to set.
     * @return This builder for chaining.
     */
    public Builder setVisitTime(
        String value) {
      if (value == null) { throw new NullPointerException(); }
      visitTime_ = value;
      bitField0_ |= 0x00000020;
      onChanged();
      return this;
    }
    /**
     * <code>string visit_time = 11;</code>
     * @return This builder for chaining.
     */
    public Builder clearVisitTime() {
      visitTime_ = getDefaultInstance().getVisitTime();
      bitField0_ = (bitField0_ & ~0x00000020);
      onChanged();
      return this;
    }
    /**
     * <code>string visit_time = 11;</code>
     * @param value The bytes for visitTime to set.
     * @return This builder for chaining.
     */
    public Builder setVisitTimeBytes(
        com.google.protobuf.ByteString value) {
      if (value == null) { throw new NullPointerException(); }
      checkByteStringIsUtf8(value);
      visitTime_ = value;
      bitField0_ |= 0x00000020;
      onChanged();
      return this;
    }
    @Override
    public final Builder setUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.setUnknownFields(unknownFields);
    }

    @Override
    public final Builder mergeUnknownFields(
        final com.google.protobuf.UnknownFieldSet unknownFields) {
      return super.mergeUnknownFields(unknownFields);
    }


    // @@protoc_insertion_point(builder_scope:LogData2)
  }

  // @@protoc_insertion_point(class_scope:LogData2)
  private static final LogData2 DEFAULT_INSTANCE;
  static {
    DEFAULT_INSTANCE = new LogData2();
  }

  public static LogData2 getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  private static final com.google.protobuf.Parser<LogData2>
      PARSER = new com.google.protobuf.AbstractParser<LogData2>() {
    @Override
    public LogData2 parsePartialFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      Builder builder = newBuilder();
      try {
        builder.mergeFrom(input, extensionRegistry);
      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        throw e.setUnfinishedMessage(builder.buildPartial());
      } catch (com.google.protobuf.UninitializedMessageException e) {
        throw e.asInvalidProtocolBufferException().setUnfinishedMessage(builder.buildPartial());
      } catch (java.io.IOException e) {
        throw new com.google.protobuf.InvalidProtocolBufferException(e)
            .setUnfinishedMessage(builder.buildPartial());
      }
      return builder.buildPartial();
    }
  };

  public static com.google.protobuf.Parser<LogData2> parser() {
    return PARSER;
  }

  @Override
  public com.google.protobuf.Parser<LogData2> getParserForType() {
    return PARSER;
  }

  @Override
  public LogData2 getDefaultInstanceForType() {
    return DEFAULT_INSTANCE;
  }

}

