package scala.util

import org.apache.flink.table.api.TableEnvironment

import scala.sql.udaf.internal._
import scala.sql.udf.internal._

object FlinkPythonUtils {

  def registryAllUdfs(tEnv: TableEnvironment): Unit = {
    tEnv.createTemporarySystemFunction("nvl", classOf[Nvl])

    tEnv.createTemporarySystemFunction("substring_index", classOf[SubstringIndex])
    tEnv.createTemporarySystemFunction("concat_ws", classOf[ConcatWs])
    tEnv.createTemporarySystemFunction("split", classOf[StringSplit])

    tEnv.createTemporarySystemFunction("get_json_object", classOf[GetJsonObject])
    tEnv.createTemporarySystemFunction("json_tuple", classOf[JsonTuple])
    tEnv.createTemporarySystemFunction("to_json", classOf[StructsToJson])
    tEnv.createTemporarySystemFunction("from_json", classOf[JsonToStructs])

    tEnv.createTemporarySystemFunction("extract_unix_timestamp", classOf[ExtractUnixTimestamp])

    tEnv.createTemporarySystemFunction("named_struct", classOf[CreateNamedStruct])

    tEnv.createTemporarySystemFunction("size", classOf[Size])
    tEnv.createTemporarySystemFunction("slice", classOf[Slice])
    tEnv.createTemporarySystemFunction("array_distinct", classOf[ArrayDistinct])
    tEnv.createTemporarySystemFunction("array_contains", classOf[ArrayContains])
    tEnv.createTemporarySystemFunction("array_max", classOf[ArrayMax])
    tEnv.createTemporarySystemFunction("array_min", classOf[ArrayMin])
    tEnv.createTemporarySystemFunction("sort_array", classOf[SortArray])

    tEnv.createTemporarySystemFunction("explode", classOf[Explode])
    tEnv.createTemporarySystemFunction("posexplode", classOf[PosExplode])

    tEnv.createTemporarySystemFunction("value_sum", classOf[ValueSumAggFunction])
    tEnv.createTemporarySystemFunction("value_count", classOf[ValueCountAggFunction])
  }

}
