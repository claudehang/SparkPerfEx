package com.microsoft.spark.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import java.util.UUID
import org.apache.spark.sql.functions.rand
import org.apache.log4j.Logger
import org.apache.log4j.Level

case class Tenant(tenantId: String, deviceId: String, tenantVal: Double)
case class Device(deviceId: String, model: String, cpuData: Double, ramData: Double)

object Ex {
  val debug = true
  var tenantPath, devicePath = ""
  val numOfTenants = 10
  val numOfDevicesPerTenant = 100

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    if (debug) {
      tenantPath = "C:\\tmp\\sto\\mema\\tenanttable"
      devicePath = "C:\\tmp\\sto\\mema\\devicetable"
    }
    else {
      tenantPath = "abfss://filesyslinhang@datalakelinhang.dfs.core.windows.net/perftest/tenanttable"
      devicePath = "abfss://filesyslinhang@datalakelinhang.dfs.core.windows.net/perftest/devicetable"
    }

    // use this for local debugging
    val spark = SparkSession
      .builder()
      .appName("demo")
      .config("spark.master", "local")
      .getOrCreate()

    // use this for online running
//    val spark = SparkSession
//      .builder()
//      .config("haha", false)
//      .getOrCreate()

    import spark.implicits._

    def generateModel(): String = {
      val CpcSku: Array[String] = Array("a", "b", "c", "d", "e", "f")
      CpcSku(scala.util.Random.nextInt(6))
    }

    def benchmark(name: String)(f: => Unit): Unit = {
      val startTime = System.nanoTime
      f
      val endTime = System.nanoTime
      println(s"Time taken in $name: " + (endTime - startTime).toDouble / 1000000000 + " seconds")
    }

    def buildTenantTable(numOfTenants: Int, numOfDevicesPerTenant: Int): Unit = {

      val tenants = List.tabulate(numOfTenants)(_ => UUID.randomUUID().toString)

      for (tenant <- tenants) {
        var tenantSeq: Seq[Tenant] = Seq.empty[Tenant]
        var deviceSeq: Seq[Device] = Seq.empty[Device]

        val devices = List.tabulate(numOfDevicesPerTenant)(_ => UUID.randomUUID().toString)

        for (device <- devices) {
          tenantSeq = tenantSeq :+ Tenant(tenant, device, scala.util.Random.nextDouble)
          deviceSeq = deviceSeq :+ Device(device, generateModel(), scala.util.Random.nextDouble, scala.util.Random.nextDouble)
          deviceSeq = deviceSeq :+ Device(device, generateModel(), scala.util.Random.nextDouble, scala.util.Random.nextDouble)
          deviceSeq = deviceSeq :+ Device(device, generateModel(), scala.util.Random.nextDouble, scala.util.Random.nextDouble)

        }

        val tenantDF = tenantSeq.toDF().orderBy(rand())
        tenantDF
          .repartition($"tenantId")
          .write
          .partitionBy("tenantId")
          .mode("append")
          .parquet(tenantPath)

        val deviceDF = deviceSeq.toDF().orderBy(rand())
        deviceDF
          .repartition($"model")
          .write
          .partitionBy("model")
          .mode("append")
          .parquet(devicePath)
      }
    }

    //    benchmark("build dataset") {
    //      buildTenantTable(numOfTenants, numOfDevicesPerTenant)
    //    }

    def readParquet(path: String) = {
      spark.read.parquet(path)
    }

    val s1 = System.nanoTime
    val tenantDf = readParquet(tenantPath)
    tenantDf.cache()
    println("tenant table rows: " + tenantDf.count())
    val e1 = System.nanoTime
    println("Time taken loading tenant table: " + (e1 - s1).toDouble / 1000000000 + " seconds")

    val s2 = System.nanoTime
    val deviceDf = readParquet(devicePath)
    deviceDf.cache()
    println("device table rows: " + deviceDf.count())
    val e2 = System.nanoTime
    println("Time taken loading device table: " + (e2 - s2).toDouble / 1000000000 + " seconds")

    def joinTables(tenantDf: DataFrame, deviceDf: DataFrame): Unit = {

      val joinTable = tenantDf.join(deviceDf, tenantDf("deviceId") ===  deviceDf("deviceId"), "leftouter")

      println("joined rows: " + joinTable.count())
    }

    benchmark("join time") {
      joinTables(tenantDf, deviceDf)
    }
  }
}