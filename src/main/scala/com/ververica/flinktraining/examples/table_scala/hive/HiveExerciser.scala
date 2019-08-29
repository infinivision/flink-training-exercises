package com.ververica.flinktraining.examples.table_scala.hive

import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.api._
import org.apache.flink.api._

object HiveExerciser {

  def main(args: Array[String]): Unit = {

    // create Blink Planner Table Environment
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build()
    val tEnv = TableEnvironment.create(settings)
    tEnv.getConfig.getConfiguration.setInteger(TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM.key, 3)

    //    val tEnv = StreamTableEnvironment.create(env, settings)
    val hiveCatalog = new HiveCatalog("infinivision_hive", "infinivision_cdp",
      "/etc/hadoop/conf", "1.2.1")

    tEnv.registerCatalog("infinivision_hive", hiveCatalog)
    tEnv.useCatalog("infinivision_hive")
    println(s"CataLogs: ${tEnv.listCatalogs()}")
    println(s"Tables: ${tEnv.listTables()}")

//    val t = tEnv.sqlQuery("SELECT COUNT(*) FROM cdp_campaign_mid_tag_ordered")

    tEnv.execute("Flink-1.9 Hive Table Read Testing")
  }

}
