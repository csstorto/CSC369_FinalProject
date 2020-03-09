import java.io.{File, PrintWriter}

import org.apache.spark.SparkContext._

import scala.io._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd._
import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.collection._
import java.time.LocalDate
import java.time.format.DateTimeFormatter

object App {

  val DAYS_IN_MONTH = 30 // let's say this for now :)

  def getNumSightingsWithinMonth(startDate: LocalDate, sightings: List[(LocalDate, Long)]): Int = {
    sightings.filter({case(date, count) => ((date.toEpochDay() - startDate.toEpochDay) <= DAYS_IN_MONTH
      && (date.toEpochDay - startDate.toEpochDay) >= 0)})
      .map({case(date, count) => count.toInt})
      .fold(0)((acc, count) => acc + count)
  }

  def getAverage(vals: List[Double]): Double = {
    val agg = vals
      .aggregate((0.0,0.0))(
        (acc, n) => (acc._1 + n, acc._2 + 1.0),
        (acc1, acc2) => (acc1._1 + acc2._1, acc1._2 + acc2._2))
    return agg._1 / agg._2
  }

  def main(args: Array[String]): Unit = {
    /* Windows only */
    System.setProperty("hadoop.home.dir", "c:/winutils/")
    /* ----------- */
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("spark01")
      /* Windows only */
      .setMaster("local[4]")
    /* ------------ */
    val sc = new SparkContext(conf)

    /* result: Tuple(date, popularity, genres) */
    val movies = sc.textFile("movies_metadata.csv")
      .map(_.split("\\[|\\]"))
      .filter(x => x(1).length > 0)
      .map(entry => (entry(2).split(",")(1), entry(2).split(",")(2), entry(1).split("(, )?\\{|\\}")
        .filter(_.length > 0)
        .map(_.split(":")(2)
          .split("'")(1))))
      .map{case (pop, date, genres) => (date.split("/")(2) + "-"
        + "%02d".format(date.split("/")(0).toInt) + "-"
        + "%02d".format(date.split("/")(1).toInt),
        pop,
        genres)}
      .filter{case (date, pop, genres) => date.split("-")(0) >= "2000"}
      .map{case (date, pop, genres) => (LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")), pop.toDouble, genres)}

    /* At the end, the sightings are counted based on the date
     * result: Tuple2(LocalDate(yyyy-MM-dd), countofSightings)*/
    val sightings = sc.textFile("scrubbed.csv")
      .map(sighting => sighting.split(" ")(0))
      .map(x => x.split("/")(2) + "-" + "%02d".format(x.split("/")(0).toInt) + "-" + "%02d".format(x.split("/")(1).toInt))
      .filter(_.split("-")(0) >= "2000")
      .countByValue().toList.sortBy(_._1)
      .map({ case (date, count) => (LocalDate.parse(date, DateTimeFormatter.ofPattern("yyyy-MM-dd")), count) })

    val pw = new PrintWriter(new File("src/UFO.csv"))
    sightings.foreach(x => pw.write(x._1 + "," + x._2 + "\r\n"))
    pw.close
    /* Get total UFO sightings within 1 mo of release for each movie, weight by popularity,
     * group by genre, get average per genre */
    val result = movies
      .map({case(date, popularity, genreList) => (genreList, popularity, getNumSightingsWithinMonth(date, sightings))})
      // Copy movie data into each individual genre of the movie
      .flatMap({case(genreList, popularity, numSightings) => genreList.map((_, popularity, numSightings))})
      // *TODO* Normalize popularity by genre
      .map({case(genre, popularity, numSightings) => (genre, popularity * numSightings)})
      .groupByKey
      .mapValues(_.toList)
      .map({case(genre, weightedSightings) => (genre, getAverage(weightedSightings))})
  }
}
