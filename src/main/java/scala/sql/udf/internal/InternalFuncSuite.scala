package scala.sql.udf.internal

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite

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

  override protected def afterAll(): Unit = {
    env.execute()
  }
}
