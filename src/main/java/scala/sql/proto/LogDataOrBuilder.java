// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: log_data.proto

package scala.sql.proto;

public interface LogDataOrBuilder extends
    // @@protoc_insertion_point(interface_extends:LogData)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>string dt = 1;</code>
   * @return The dt.
   */
  String getDt();
  /**
   * <code>string dt = 1;</code>
   * @return The bytes for dt.
   */
  com.google.protobuf.ByteString
      getDtBytes();

  /**
   * <pre>
   * Unique ID number for this person.
   * </pre>
   *
   * <code>string bs = 2;</code>
   * @return The bs.
   */
  String getBs();
  /**
   * <pre>
   * Unique ID number for this person.
   * </pre>
   *
   * <code>string bs = 2;</code>
   * @return The bytes for bs.
   */
  com.google.protobuf.ByteString
      getBsBytes();

  /**
   * <code>string report_time = 3;</code>
   * @return The reportTime.
   */
  String getReportTime();
  /**
   * <code>string report_time = 3;</code>
   * @return The bytes for reportTime.
   */
  com.google.protobuf.ByteString
      getReportTimeBytes();

  /**
   * <code>string browser_uniq_id = 4;</code>
   * @return The browserUniqId.
   */
  String getBrowserUniqId();
  /**
   * <code>string browser_uniq_id = 4;</code>
   * @return The bytes for browserUniqId.
   */
  com.google.protobuf.ByteString
      getBrowserUniqIdBytes();

  /**
   * <code>string os_plant = 5;</code>
   * @return The osPlant.
   */
  String getOsPlant();
  /**
   * <code>string os_plant = 5;</code>
   * @return The bytes for osPlant.
   */
  com.google.protobuf.ByteString
      getOsPlantBytes();

  /**
   * <code>string page_id = 6;</code>
   * @return The pageId.
   */
  String getPageId();
  /**
   * <code>string page_id = 6;</code>
   * @return The bytes for pageId.
   */
  com.google.protobuf.ByteString
      getPageIdBytes();

  /**
   * <code>string page_name = 7;</code>
   * @return The pageName.
   */
  String getPageName();
  /**
   * <code>string page_name = 7;</code>
   * @return The bytes for pageName.
   */
  com.google.protobuf.ByteString
      getPageNameBytes();

  /**
   * <code>string page_param = 8;</code>
   * @return The pageParam.
   */
  String getPageParam();
  /**
   * <code>string page_param = 8;</code>
   * @return The bytes for pageParam.
   */
  com.google.protobuf.ByteString
      getPageParamBytes();

  /**
   * <code>int64 item_id = 9;</code>
   * @return The itemId.
   */
  long getItemId();

  /**
   * <code>int32 item_type = 10;</code>
   * @return The itemType.
   */
  int getItemType();

  /**
   * <code>string visit_time = 11;</code>
   * @return The visitTime.
   */
  String getVisitTime();
  /**
   * <code>string visit_time = 11;</code>
   * @return The bytes for visitTime.
   */
  com.google.protobuf.ByteString
      getVisitTimeBytes();
}
