package scala.sql.timeattr

case class OnlineLog
(
  pageId: String,
  userId: String,
  eventTime: Long,
  time: Long,
  eventTimeStr: String,
  timeStr: String,
  var visitCnt: Int
)
