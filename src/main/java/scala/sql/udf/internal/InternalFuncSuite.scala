package scala.sql.udf.internal

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.data.RowData
import org.apache.flink.types.Row
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

import scala.connector.log.LogSinkFunction
import scala.sql.utils.TableImplicits._

class InternalFuncSuite extends AnyFunSuite with BeforeAndAfterAll{
  var env: StreamExecutionEnvironment = _
  var tEnv: StreamTableEnvironment = _

  override protected def beforeAll(): Unit = {
    val conf = new Configuration()
    env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf)
    env.setParallelism(1)
    env.getConfig.enableObjectReuse()

    val settings = EnvironmentSettings.newInstance().inStreamingMode().build()
    tEnv = StreamTableEnvironment.create(env, settings)
  }

  test("nvl2"){
    tEnv.createTemporarySystemFunction("nvl", classOf[Nvl])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      cnt bigint,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.3',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.code.null-rate' = '0.3',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        code,
        cnt,
        -- 可以提示类型转换
        nvl(name, '2') name1,
        nvl(code, cnt) code2,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.getResolvedSchema.toSinkRowDataType
    //rstTable.printSchema()
    //println(rstTable.explain())

    rstTable.execute().print()
  }

  test("nvl"){
    tEnv.createTemporarySystemFunction("nvl", classOf[Nvl])
    tEnv.createTemporarySystemFunction("named_struct", classOf[CreateNamedStruct])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      code int,
      cnt bigint,
      data row<name string, age int>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.3',
      'fields.code.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.code.null-rate' = '0.3',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.3',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        code,
        cnt,
        -- 可以提示类型转换
        nvl(name, '2') name1,
        nvl(code, 111) code1,
        nvl(code, cnt) code2,
        nvl(cnt, 66666666666) cnt1,
        nvl(cnt, code) cnt2,
        data,
        nvl(data, null) data1,
        nvl(data, named_struct('name', name, 'age', age)) data2,
        -- 对应的类型相同FieldsDataType的equals返回true
        nvl(data, named_struct('name1', name, 'age1', age)) data3,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("substring_index"){
    tEnv.createTemporarySystemFunction("substring_index", classOf[SubstringIndex])

    var sql = """
    CREATE TABLE tmp_tb1 (
      time_str string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.time_str.expression' = '#{date.past ''15'',''SECONDS''}',
      'fields.name.null-rate' = '0.3',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        time_str,
        substring_index(time_str, '-', 1) str1,
        substring_index(time_str, '-', 2) str2,
        substring_index(time_str, '-', 3) str3,
        substring_index(time_str, ':', -1) str_1,
        substring_index(time_str, ':', -2) str_2,
        substring_index(time_str, ' ', -1) str_3,
        proctime
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("to_json"){
    tEnv.createTemporarySystemFunction("named_struct", new CreateNamedStruct)
    tEnv.createTemporarySystemFunction("to_json", classOf[StructsToJson])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        datas,
        to_json(named_struct('name', name, 'age', age, 'cnt', cnt)) json,
        to_json(names) names_json,
        to_json(datas) datas_json,
        to_json(array[name, null, name]) info,
        to_json(array[named_struct('age', age, 'cnt', cnt), null]) info2
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("from_json"){
    tEnv.createTemporarySystemFunction("named_struct", new CreateNamedStruct)
    tEnv.createTemporarySystemFunction("to_json", classOf[StructsToJson])
    tEnv.createTemporarySystemFunction("from_json", classOf[JsonToStructs])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      data row<name string, age int>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.data.name.expression' = '#{harry_potter.spell}',
      'fields.data.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        to_json(data) data_json,
        from_json(to_json(data), 'row<name string, age int>') json,
        from_json(to_json(datas), 'array<row<name string, age int>>') array_json,
        from_json(to_json(names), 'array<string>') array_json2
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  test("get_json_object"){
    tEnv.createTemporarySystemFunction("named_struct", new CreateNamedStruct)
    tEnv.createTemporarySystemFunction("get_json_object", new GetJsonObject)
    tEnv.createTemporarySystemFunction("to_json", classOf[StructsToJson])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        json,
        get_json_object(json, '$.name') json_name,
        names_json,
        get_json_object(names_json, '$[1]') names_json_p1,
        datas_json,
        get_json_object(datas_json, '$[0].age') datas_json_p0
    from(
        select
            names,
            datas,
            to_json(named_struct('name', name, 'age', age, 'cnt', cnt)) json,
            to_json(names) names_json,
            to_json(datas) datas_json,
            to_json(array[name, null, name]) info,
            to_json(array[named_struct('age', age, 'cnt', cnt), null]) info2
        from tmp_tb1
    ) t
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("json_tuple"){
    tEnv.createTemporarySystemFunction("named_struct", new CreateNamedStruct)
    tEnv.createTemporarySystemFunction("json_tuple", new JsonTuple)
    tEnv.createTemporarySystemFunction("to_json", classOf[StructsToJson])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        json,
        name,
        age,
        json2,
        t2.names
    from(
        select
            names,
            to_json(named_struct('name', name, 'age', age, 'cnt', cnt)) json,
            to_json(named_struct('names', names, 'age', age)) json2
        from tmp_tb1
    ) t
    left join lateral table(json_tuple(json, 'name', 'age')) as t1(name, age) on true
    left join lateral table(json_tuple(json2, 'names')) as t2(names) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("explode array"){
    tEnv.createTemporarySystemFunction("explode", classOf[Explode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        age,
        names,
        sub_name,
        proctime
    from tmp_tb1
    left join lateral table(explode(names)) as t1(sub_name) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("posexplode array"){
    tEnv.createTemporarySystemFunction("posexplode", classOf[PosExplode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names array<string>,
      datas array<row<name string, age int>>,
      log_time timestamp_ltz(3),
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{harry_potter.spell}',
      'fields.names.length' = '3',
      'fields.names.null-rate' = '0.3',
      'fields.datas.name.expression' = '#{harry_potter.spell}',
      'fields.datas.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datas.length' = '3',
      'fields.datas.null-rate' = '0.3',
      'fields.log_time.expression' =  '#{date.past ''5'',''0'',''SECONDS''}',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        datas,
        i,
        data,
        data.name name,
        data.age age
    from tmp_tb1
    left join lateral table(posexplode(datas)) as t1(i, data) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("explode map"){
    tEnv.createTemporarySystemFunction("explode", classOf[Explode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      counts map<string, int>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.counts.key.expression' = '#{harry_potter.spell}',
      'fields.counts.value.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.counts.length' = '3',
      'fields.counts.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        counts,
        name,
        cnt,
        proctime
    from tmp_tb1
    left join lateral table(explode(counts)) as t1(name, cnt) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("posexplode map"){
    tEnv.createTemporarySystemFunction("posexplode", classOf[PosExplode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      counts map<string, int>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.counts.key.expression' = '#{harry_potter.spell}',
      'fields.counts.value.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.counts.length' = '3',
      'fields.counts.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        counts,
        i,
        name,
        cnt,
        proctime
    from tmp_tb1
    left join lateral table(posexplode(counts)) as t1(i, name, cnt) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
    /*rstTable.toAppendStream[Row].addSink{row =>
      println(row)
    }*/

  }

  test("explode multiset"){
    tEnv.createTemporarySystemFunction("explode", classOf[Explode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      names multiset<string>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.names.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.names.length' = '4',
      'fields.names.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        sub_name,
        proctime
    from tmp_tb1
    left join lateral table(explode(names)) as t1(sub_name) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  test("posexplode multiset"){
    tEnv.createTemporarySystemFunction("posexplode", classOf[PosExplode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      names multiset<string>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.names.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.names.length' = '4',
      'fields.names.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        i,
        sub_name,
        proctime
    from tmp_tb1
    left join lateral table(posexplode(names)) as t1(i, sub_name) on true
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  // multiset只有字符串类型元素可以转，相当于map，key必须为string
  test("multiset_to_json"){
    tEnv.createTemporarySystemFunction("explode", classOf[Explode])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      age int,
      cnt bigint,
      names multiset<string>,
      ages multiset<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.age.expression' = '#{number.numberBetween ''0'',''20''}',
      'fields.age.null-rate' = '0.2',
      'fields.cnt.expression' = '#{number.numberBetween ''0'',''20000000000''}',
      'fields.cnt.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.names.length' = '4',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'fields.ages.null-rate' = '0.2',
      'fields.names.null-rate' = '0.2',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        age,
        cnt,
        names
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.toAppendStream[RowData].addSink(new LogSinkFunction(
      "print",
      rstTable.getJsonRowDataSerializationSchema
    )).setParallelism(1)
  }

  test("listagg"){
    var sql = """
    CREATE TABLE tmp_tb1 (
      id string,
      name string,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.id.expression' = '#{regexify ''(1){1}''}',
      'fields.name.expression' = '#{regexify ''(莫南|青丝|璇音|流沙){1}''}',
      'fields.name.null-rate' = '0.2',
      'rows-per-second' = '5'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        id,
        listagg(name) names
    from tmp_tb1
    group by id
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }

  /**
   * org.apache.flink.table.runtime.typeutils.TypeCheckUtils#isComparable
   * return !isRaw(type) && !isMap(type) && !isMultiset(type) && !isRow(type) && !isArray(type)
   */
  test("greatest"){
    tEnv.createTemporarySystemFunction("nvl", classOf[Nvl])
    tEnv.createTemporarySystemFunction("named_struct", classOf[CreateNamedStruct])

    var sql = """
    CREATE TABLE tmp_tb1 (
      data1 row<name string, age int>,
      data2 row<name string, age int>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.data1.name.expression' = '#{harry_potter.spell}',
      'fields.data1.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data1.null-rate' = '0.1',
      'fields.data2.name.expression' = '#{harry_potter.spell}',
      'fields.data2.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.data2.null-rate' = '0.1',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        data1,
        data2,
        greatest(data1, data2) data
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()
    println(rstTable.explain())

    rstTable.execute().print()
  }


  test("sort_array"){
    tEnv.createTemporarySystemFunction("sort_array", classOf[SortArray])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages array<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        ages,
        sort_array(ages) ages1,
        sort_array(ages, false) ages2,
        names,
        sort_array(names) names1,
        sort_array(names, false) names2
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
    /*rstTable.toAppendStream[RowData].addSink(new LogSinkFunction(
      "print",
      rstTable.getJsonRowDataSerializationSchema
    )).setParallelism(1)*/
  }

  test("sort_array_row"){
    tEnv.createTemporarySystemFunction("sort_array", classOf[SortArray])

    var sql = """
    CREATE TABLE tmp_tb1 (
      datasa array<row<name string, age int>>,
      datasb array<row<age int, name string>>,
      proctime as proctime()
    ) WITH (
      'connector' = 'faker',
      'fields.datasa.name.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.datasa.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datasa.length' = '3',
      'fields.datasa.null-rate' = '0.1',
      'fields.datasb.name.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.datasb.age.expression' = '#{number.numberBetween ''20'',''30''}',
      'fields.datasb.length' = '3',
      'fields.datasb.null-rate' = '0.1',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        datasa,
        sort_array(datasa) datasa1,
        datasb,
        sort_array(datasb) datasb1
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    //rstTable.execute().print()

    rstTable.toAppendStream[RowData].addSink(new LogSinkFunction(
      "print",
      rstTable.getJsonRowDataSerializationSchema
    ))
  }

  test("concat_ws"){
    tEnv.createTemporarySystemFunction("concat_ws", classOf[ConcatWs])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages array<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        name,
        names,
        concat_ws(',', '哈哈', names) names1,
        concat_ws(',', '哈哈', name) names2,
        concat_ws(',', '哈哈', names, name) names3
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
  }

  test("size"){
    tEnv.createTemporarySystemFunction("size", classOf[Size])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages multiset<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        size(names) names_size,
        ages,
        size(ages) ages_size
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
  }

  test("slize"){
    tEnv.createTemporarySystemFunction("slize", classOf[Slice])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages array<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.names.null-rate' = '0.1',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'fields.ages.null-rate' = '0.1',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        slize(names, 1, 3) names_slize,
        ages,
        slize(ages, 1, 2) ages_slize
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  test("array_distinct"){
    tEnv.createTemporarySystemFunction("array_distinct", classOf[ArrayDistinct])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages multiset<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.names.null-rate' = '0.1',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'fields.ages.null-rate' = '0.1',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        array_distinct(names) names_distinct,
        ages,
        array_distinct(ages) ages_distinct
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)

    rstTable.execute().print()
  }

  test("array_max"){
    tEnv.createTemporarySystemFunction("array_max", classOf[ArrayMax])
    tEnv.createTemporarySystemFunction("array_min", classOf[ArrayMin])

    var sql = """
    CREATE TABLE tmp_tb1 (
      name string,
      names array<string>,
      ages array<int>
    ) WITH (
      'connector' = 'faker',
      'fields.name.expression' = '#{superhero.name}',
      'fields.name.null-rate' = '0.2',
      'fields.names.expression' = '#{regexify ''(aaa|bb|abc|abd){1}''}',
      'fields.names.length' = '4',
      'fields.names.null-rate' = '0.1',
      'fields.ages.expression' = '#{number.numberBetween ''5'',''10''}',
      'fields.ages.length' = '5',
      'fields.ages.null-rate' = '0.1',
      'rows-per-second' = '1'
    )
    """
    tEnv.executeSql(sql)

    sql = """
    select
        names,
        array_max(names) names_max,
        array_min(names) names_min,
        ages,
        array_max(ages) ages_max,
        array_min(ages) ages_min
    from tmp_tb1
    """
    val rstTable = tEnv.sqlQuery(sql)
    rstTable.printSchema()

    rstTable.execute().print()
  }

  override protected def afterAll(): Unit = {
    env.execute()
  }
}

