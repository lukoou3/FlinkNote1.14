// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: log_data.proto

package scala.sql.proto;

public final class LogDataProto {
  private LogDataProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_LogData_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_LogData_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    String[] descriptorData = {
      "\n\016log_data.proto\"\321\001\n\007LogData\022\n\n\002dt\030\001 \001(\t" +
      "\022\n\n\002bs\030\002 \001(\t\022\023\n\013report_time\030\003 \001(\t\022\027\n\017bro" +
      "wser_uniq_id\030\004 \001(\t\022\020\n\010os_plant\030\005 \001(\t\022\017\n\007" +
      "page_id\030\006 \001(\t\022\021\n\tpage_name\030\007 \001(\t\022\022\n\npage" +
      "_param\030\010 \001(\t\022\017\n\007item_id\030\t \001(\003\022\021\n\titem_ty" +
      "pe\030\n \001(\005\022\022\n\nvisit_time\030\013 \001(\tB!\n\017scala.sq" +
      "l.protoB\014LogDataProtoP\001b\006proto3"
    };
    com.google.protobuf.Descriptors.FileDescriptor.InternalDescriptorAssigner assigner =
        new com.google.protobuf.Descriptors.FileDescriptor.    InternalDescriptorAssigner() {
          public com.google.protobuf.ExtensionRegistry assignDescriptors(
              com.google.protobuf.Descriptors.FileDescriptor root) {
            descriptor = root;
            return null;
          }
        };
    com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        }, assigner);
    internal_static_LogData_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_LogData_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_LogData_descriptor,
        new String[] { "Dt", "Bs", "ReportTime", "BrowserUniqId", "OsPlant", "PageId", "PageName", "PageParam", "ItemId", "ItemType", "VisitTime", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
