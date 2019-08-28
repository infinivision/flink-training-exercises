package com.ververica.flinktraining.examples.table_scala.hive

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog

object HiveExerciser {
  // set up streaming execution environment
  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

  // create TableEnvironment
  val tEnv = StreamTableEnvironment.create(env)
  val hiveCatalog = new HiveCatalog("infinivision_hive", "infinivision_cdp",
    "/etc/hadoop/conf", "1.2.1")

  tEnv.registerCatalog("infinivision_hive", hiveCatalog)
  tEnv.useCatalog("infinivision_hive")
  println(s"CataLogs: ${tEnv.listCatalogs()}")
  println(s"Tables: ${tEnv.listTables()}")

  env.execute("Flink-1.9 Hive Table Read Testing")
}
