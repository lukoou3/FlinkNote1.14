// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: log_data2.proto

package scala.sql.proto;

public final class LogData2Proto {
  private LogData2Proto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_LogData2_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_LogData2_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\017log_data2.proto\"u\n\010LogData2\022\n\n\002dt\030\001 \001(" +
      "\t\022\n\n\002bs\030\002 \001(\t\022\023\n\013report_time\030\003 \001(\t\022\017\n\007it" +
      "em_id\030\t \001(\003\022\021\n\titem_type\030\n \001(\005\022\022\n\nvisit_" +
      "time\030\013 \001(\tJ\004\010\004\020\tB\"\n\017scala.sql.protoB\rLog" +
      "Data2ProtoP\001b\006proto3"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_LogData2_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_LogData2_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_LogData2_descriptor,
        new String[] { "Dt", "Bs", "ReportTime", "ItemId", "ItemType", "VisitTime", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}