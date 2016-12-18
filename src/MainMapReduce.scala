/**
  * Created by jorge on 16/12/16.
  */


import org.apache.spark.MapOutputTracker
import org.joda.time.format.DateTimeFormatter

import scala.util.Try

/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.joda.time.format.DateTimeFormat
import org.joda.time.Minutes
import org.joda.time.Seconds
import scala.collection.mutable.ListBuffer
import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.mllib.fpm.PrefixSpan
import org.apache.spark.sql.functions.{collect_list, collect_set}
import java.io._
import org.jfree.chart._
import org.jfree._
import org.jfree.data.xy._
import org.jfree.data.category.CategoryDataset
import org.jfree.data.category.DefaultCategoryDataset
import org.jfree.chart.{ChartFactory,ChartPanel}
import org.jfree.chart.plot.PlotOrientation
import scalax.chart._
import scalax.chart.api._
import org.joda.time.DateTime
import org.joda.time.Seconds
import org.joda.time.format.DateTimeFormat

object SimpleMapApp {

  val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
  val sc = new SparkContext(conf)
  val spark = SparkSession
    .builder()
    .appName("Query")
    .enableHiveSupport()
    .config(conf)
    .getOrCreate()

  def toInt(s: String): Int = {
    try {
      s.toInt
    } catch {
      case e: Exception => 0
    }
  }

  var hps_list = new ListBuffer[(String,Double)]()
  var dps_list = new ListBuffer[(String,Double)]()

  val hps_csv_name = "/home/jorge/logs/hps.csv"
  val dps_csv_name = "/home/jorge/logs/dps.csv"

  val total_hps_csv_name = "/home/jorge/logs/total_hps.csv"
  val total_dps_csv_name = "/home/jorge/logs/total_dps.csv"



  def saveDfToCsv(df: DataFrame, tsvOutput: String,
                  sep: String = ",", header: Boolean = true): Unit = {
    val tmpParquetDir = "/home/jorge/logs/TMP"
    df.repartition(1).write.
      format("com.databricks.spark.csv").
      option("header", header.toString).
      option("delimiter", sep).
      save(tmpParquetDir)
    val dir = new File(tmpParquetDir)
    val tmpTsvFile = tmpParquetDir + File.separatorChar + "part-00000"
    println(tsvOutput)
    (new File(tmpTsvFile)).renameTo(new File(tsvOutput))
    dir.listFiles.foreach( f => f.delete )
    dir.delete
  }


  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val logFile = "/home/jorge/logs/log1.csv" // Should be some file on your system


    println("Lendo o log...")

    val text_file = sc.textFile(logFile)

    println("Log lido com sucesso!")
    val map_com_numero_inicio = text_file.zipWithIndex().map{ case( line,i ) => (i.toString , line.split(",")(0) , line) }
    val resultado_encontro_inicio_rdd = map_com_numero_inicio.filter{ case (a,b,c) => b.contains("ENCOUNTER_START") }
    val resultado_encontro_inicio_first = resultado_encontro_inicio_rdd.first()
    val resultado_encontro_inicio = resultado_encontro_inicio_first._1.toInt
    val formatter = DateTimeFormat.forPattern("MM/dd HH:mm:ss")
    //val dt = formatter.parseDateTime(resultado_encontro_inicio_first._3.split(",")(0).split(" ")(1).dropRight(4))
    //println(dt)
    val horario_encontro_inicio =    formatter.parseDateTime(resultado_encontro_inicio_first._3.split(",")(0).split("  ")(0).dropRight(4))


    val resultado_encontro_fim_rdd = map_com_numero_inicio.filter{ case (a,b,c) => b.contains("ENCOUNTER_END") }
    val resultado_encontro_fim_first = resultado_encontro_fim_rdd.first()
    val resultado_encontro_fim = resultado_encontro_fim_first._1.toInt

    val horario_encontro_fim =   formatter.parseDateTime(resultado_encontro_fim_first._3.split(",")(0).split("  ")(0).dropRight(4))


    val duracao_encontro = Seconds.secondsBetween(horario_encontro_inicio,horario_encontro_fim).getSeconds

    val encounterBoss = resultado_encontro_inicio_rdd.first()._3.split(",")(2)


    println("teste")
    println(resultado_encontro_inicio)
    println(resultado_encontro_fim)
    println(encounterBoss)
    //println(horario_encontro_inicio)
    println(duracao_encontro)


    val encontro = map_com_numero_inicio.filter{ case (a,b,c) => (a.toInt > resultado_encontro_inicio && a.toInt < resultado_encontro_fim && !(c.split(",")(2).contains(encounterBoss)))}
    val heals = encontro.filter{case (a,b,c) => c.contains("_HEAL")}.map{ case(a,b,c) => (c.split(",")(2),c.split(",")(25).toInt)}
    val damage = encontro.filter{case (a,b,c) => c.contains("_DAMAGE")}.map{ case(a,b,c) => (c.split(",")(2),c.split(",")(25).toInt)}


    val damage_gsp = encontro.filter{case (a,b,c) => c.contains("SPELL_DAMAGE") && c.contains("Boyardee-Blackhand")}.map{ case(a,b,c) => c.split(",")(10)}
    val damage_gsp_with_line = damage_gsp.zipWithIndex().map{ case( line,i ) => (i.toInt , line) }

    val damage_gsp_with_line_minus_1 = damage_gsp_with_line.map { case (i, line) => (i.toInt - 1, line) }
    val damage_gsp_with_line_minus_2 = damage_gsp_with_line.map { case (i, line) => (i.toInt - 2, line) }
    val damage_gsp_with_line_minus_3 = damage_gsp_with_line.map { case (i, line) => (i.toInt - 3, line) }
    val damage_gsp_with_line_minus_4 = damage_gsp_with_line.map { case (i, line) => (i.toInt - 4, line) }


      //val damage_gps_intersection = damage_gsp_with_line.join(damage_gsp_with_line_minus)
    val damage_gps_join = damage_gsp_with_line.join(damage_gsp_with_line_minus_1)
    val damage_gps_reduce_com_x = damage_gps_join.map { case (a, b) => (b, 1) }.reduceByKey(_ + _).filter { case (a, b) => b > 5 }
    val damage_gps_reduce_com_dois_set = damage_gps_reduce_com_x.map { case (a, b) => a }.collect()
      //val damage_gps_to_set = damage_gps_reduce_com_dois_set.

    val damage_gps_intersection = damage_gps_join.filter { case(a,b) => damage_gps_reduce_com_dois_set.contains(b)}
    println("Sequência de " + 1 + ":")
    damage_gps_reduce_com_x.foreach(x => println(x))


    val damage_gps_join_2 = damage_gps_intersection.join(damage_gsp_with_line_minus_2)
    val damage_gps_reduce_com_x_2 = damage_gps_join_2.map{ case (a, b) => (b, 1) }.reduceByKey(_ + _).filter { case (a, b) => b > 5 }
    if(damage_gps_reduce_com_x_2.count() > 0) {
      val damage_gps_reduce_com_dois_set_2 = damage_gps_reduce_com_x_2.map { case (a, b) => a }.collect()

      val damage_gps_intersection_2 = damage_gps_join_2.filter { case (a, b) => damage_gps_reduce_com_dois_set.contains(b) }
      println("Sequência de " + 2 + ":")
      damage_gps_reduce_com_x_2.foreach(x => println(x))



      val damage_gps_join_3 = damage_gps_intersection_2.join(damage_gsp_with_line_minus_2)
      val damage_gps_reduce_com_x_3 = damage_gps_join_3.map { case (a, b) => (b, 1) }.reduceByKey(_ + _).filter { case (a, b) => b > 5 }

      if(damage_gps_reduce_com_x_3.count() > 0) {
        val damage_gps_reduce_com_dois_set_3 = damage_gps_reduce_com_x_3.map { case (a, b) => a }.collect()

        val damage_gps_intersection_3 = damage_gps_join_3.filter { case (a, b) => damage_gps_reduce_com_dois_set.contains(b) }
        println("Sequência de " + 3 + ":")
        damage_gps_reduce_com_x_3.foreach(x => println(x))


        val damage_gps_join_4 = damage_gps_intersection_3.join(damage_gsp_with_line_minus_2)
        val damage_gps_reduce_com_x_4 = damage_gps_join_4.map { case (a, b) => (b, 1) }.reduceByKey(_ + _).filter { case (a, b) => b > 5 }
        if(damage_gps_reduce_com_x_4.count() > 0) {
          val damage_gps_reduce_com_dois_set_4 = damage_gps_reduce_com_x_4.map { case (a, b) => a }.collect()
          //val damage_gps_to_set = damage_gps_reduce_com_dois_set.

          val damage_gps_intersection_4 = damage_gps_join_4.filter { case (a, b) => damage_gps_reduce_com_dois_set.contains(b) }
          println("Sequência de " + 4 + ":")
          damage_gps_reduce_com_x_4.foreach(x => println(x))

          println("a maior sequência foi 4")
        }
        else println("A maior sequência foi 3")
      }else println("A maior sequência foi 2")
    }else println("A maior sequência foi 1")



    println(damage_gps_intersection.first())

    val heal_total = heals.reduceByKey(_ + _)
    val damage_total = damage.reduceByKey(_ + _)

    println("Heal total:" + heal_total.first())
    println("Dano total:" + damage_total.first())
    //val map_com_heal = map_com_numero_inicio.reduce()


  }

  def printa_HPS(x: String,seconds_between: Integer)
  {
    if( !(x.isEmpty()))
    {

      val heal = spark.sql("SELECT * FROM wow_log where lower(c0) like lower('%_heal%') and lower(c2) like lower(\"%" + x + "%\") order by horario asc")

      if(heal.count() > 1)
      {
        //heal.show()
        //sqlDF.foreach { x => println(x) }
        heal.toDF().createOrReplaceTempView("wow_hps")
        val hps = spark.sql("select sum(c25) from wow_hps")
        val heal_final = hps.first().getDouble(0)



        var hps_final = heal_final / seconds_between


        //println("o hps do %s foi %s".format(x,hps_final))
        var tmp = (x,hps_final)
        hps_list += tmp
      }
    }
  }

  def printa_DPS(x: String,seconds_between: Integer)
  {
    if( !(x.isEmpty()))
    {

      val damage = spark.sql("SELECT * FROM wow_log where lower(c0) like lower('%_damage%') and lower(c2) like lower(\"%" + x + "%\") order by horario asc")

      if(damage.count() > 1)
      {
        //heal.show()
        //sqlDF.foreach { x => println(x) }
        damage.toDF().createOrReplaceTempView("wow_dps")
        val dps = spark.sql("select sum(c25) from wow_dps")
        val damage_final = dps.first().getDouble(0)



        var dps_final = damage_final / seconds_between


        //println("o hps do %s foi %s".format(x,hps_final))
        var tmp = (x,dps_final)
        dps_list += tmp


        //val damage_skills = spark.sql("SELECT c10 FROM wow_dps order by horario asc")
        //calcula_rotacao(x,damage_skills)
      }
    }
  }


  def calcula_rotacao(player: String,data_set: DataFrame)
  {

    var skills = new ListBuffer[String]

    data_set.toDF().createOrReplaceTempView("damages")
    val skills_data_set = spark.sql("select distinct(c10) from damages")
    //skills_data_set.foreach { x => skills += x.getString(0) }


  }


  def adiciona_hps(player: String, hps: Double)
  {
    var tmp = (player,hps)
    hps_list += tmp
  }

  def adiciona_dps(player: String, dps: Double)
  {
    var tmp = (player,dps)
    dps_list += tmp
  }



}