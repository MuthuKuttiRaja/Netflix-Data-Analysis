package org.example.helloworld

import scala.App
import org.apache.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.col

import scala.collection.mutable
import scala.util.control.Breaks

class Movie(val movieId:String, val publication:Int, val movieName:String) extends Serializable
class UserRating(val movieId: String, val userId: String, val rating: Int, val ratingDate: String) extends Serializable
class MovieStat(val movieId: String, val movieName: String, val publication: Int, val ratingsCount: Long, val averageRating: Float) extends Serializable
class UserStat(val userId: String, val highestRatedFilms: Array[UserRating], val intersectCount: Int, val mMoviesAvgRating: Float) extends Serializable
class Result(val userId: String, val highestRatedFilm: String, val mMoviesAvgRating: Float, val yearRelease:Int, val ratingDt: String )

object UtilityFunctions extends Serializable {
  def getMoviesDict(value: Array[org.apache.spark.sql.Row] ): Map[String,Movie] =   {
    var obj: Map[String, Movie] = Map()
    var lineSplits: Array[String] = Array()
    for(i <- value.indices){
      lineSplits = value(i)(0).toString.split(",")
      obj+= (lineSplits(0).trim -> new Movie(lineSplits(0).trim, if (lineSplits(1)!="NULL") lineSplits(1).trim.toInt else 0, lineSplits(2).trim))
    }
    obj
  }

  def getTopMoviesList(value: Array[org.apache.spark.sql.Row]): Set[String] = {
    var moviesList:Set[String] = Set()
    for (i<-value.indices){
      moviesList+=value(i)(0).toString
    }
    moviesList
  }

  def printResult(result: Array[Result]):Unit ={
    for (i<-result.indices){
      println(result(i).userId, result(i).highestRatedFilm, result(i).yearRelease, result(i).ratingDt)
    }
  }

  def getTopMUsers(M:Int, U:Int, userStatsArray: Array[UserStat], moviesDict: Map[String,Movie]): Array[Result] = {

    val topUsers = userStatsArray.sortBy(r => (r.mMoviesAvgRating, r.userId.toInt))
    var results: Array[Result] = Array()
    var userRatings: Array[(String, String, Int)] = Array()
    var ratingsArray: Array[UserRating] = Array()

    val loop = new Breaks
    loop.breakable{
      for (i <- topUsers.indices) {
        if (results.length == U) {
          loop.break
        }
        userRatings = Array()
        ratingsArray = topUsers(i).highestRatedFilms
        for (j <- ratingsArray.indices) {
          val rating = ratingsArray(j)
          userRatings :+= (rating.ratingDate, moviesDict(rating.movieId).movieName, moviesDict(rating.movieId).publication)
        }
        val highestRatedFilm = userRatings.sortBy(r => (-r._3, r._2))
        if (topUsers(i).intersectCount == M) {
          results :+= new Result(topUsers(i).userId, highestRatedFilm(0)._2, topUsers(i).mMoviesAvgRating, highestRatedFilm(0)._3, highestRatedFilm(0)._1)
        }
      }
    }
    results
  }
}

object SimpleApp {
  def main(args: Array[String]): Unit = {
    val conf = new spark.SparkConf().setAppName("scala spark")
    val sc = new SparkContext(conf)
    val sqlContext: SQLContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._
    val mConfBroadcast = sc.broadcast(5)
    val rConfBroadcast = sc.broadcast(10)
    val UConfBroadcast = sc.broadcast(25)

    val moviesRefRDD = sc.textFile("*")
    val moviesRDD = sc.wholeTextFiles("*")



    val movieRefMapperRDD = moviesRefRDD.map(value => {
      val splits: Array[String] = value.split(',')
      (splits(0), new Movie(splits(0), if (splits(1) != "NULL") splits(1).toInt else 0, splits(2)))
    }).groupByKey()

    val parseCSVMapperRDD = moviesRDD.flatMap(value=>{
      var movieId: String = ""
      val lineSplits: Array[String] = value._2.split('\n')
      var out: Array[UserRating] = Array()
      var valueSplits: Array[String] = Array()

      for (i <- lineSplits.indices) {
        if (i == 0) {
          movieId = lineSplits(i).trim.stripSuffix(":")
        }
        else {
          valueSplits = lineSplits(i).split(",")
          if (valueSplits.length == 3) {
            out :+= new UserRating(movieId, valueSplits(0).trim, if (valueSplits(1)!="NULL") valueSplits(1).trim.toInt else 0, valueSplits(2).trim)
          }
        }
      }
      out
    }).cache()

    val moviesMapperRDD = parseCSVMapperRDD.map(value=>{
      (value.movieId, value)
    }).groupByKey()

    val movieReducerRDD = moviesMapperRDD.join(movieRefMapperRDD).map(value=>{
      val ratingsIter: Iterator[UserRating] = value._2._1.iterator
      val movieIter: Iterator[Movie] = value._2._2.iterator
      var count: Long = 0
      var ratingTotal: Long = 0
      var movieName: String = ""
      var moviePublication: Int = 0

      while (movieIter.hasNext) {
        val movieObj: Movie = movieIter.next()
        movieName = movieObj.movieName
        moviePublication = movieObj.publication
      }

      while (ratingsIter.hasNext) {
        val ratingsObj: UserRating = ratingsIter.next()
        count += 1
        ratingTotal += ratingsObj.rating
      }
      val average: Float = ratingTotal / count
      (value._1, movieName, moviePublication, count, average)
    })

    val dfRDD = movieReducerRDD.toDF("movieId", "movieName", "publication", "ratingsCount", "averageRating")
    val dfRDD1 = dfRDD.sort(col("ratingsCount").desc, col("publication").desc, col("movieName").asc).filter(dfRDD("ratingsCount")>rConfBroadcast.value).take(mConfBroadcast.value)
    val movies_lst = UtilityFunctions.getTopMoviesList(dfRDD1)
    val broadcastMoviesList = sc.broadcast(movies_lst)

    val userMapperRDD = parseCSVMapperRDD.map(value=>{
      (value.userId, value)
    }).groupByKey()

    val userReducerRDD = userMapperRDD.map(value=> {
      val ratingsIter: Iterator[UserRating] = value._2.iterator
      val ratingsDict = new mutable.HashMap[Int, Array[UserRating]]() {
        override def default(key: Int): Array[UserRating] = Array()
      }
      var ratingSum: Long = 0
      var intersectCount: Int = 0

      while (ratingsIter.hasNext) {
        val ratingsObj: UserRating = ratingsIter.next()
        ratingsDict(ratingsObj.rating) :+= ratingsObj
        if (broadcastMoviesList.value.contains(ratingsObj.movieId)) {
          intersectCount += 1
          ratingSum += ratingsObj.rating
        }
      }
      new UserStat(value._1, if (ratingsDict.nonEmpty) ratingsDict(ratingsDict.keys.iterator.max) else Array(), intersectCount, if (intersectCount > 0) {
        ratingSum.toFloat / intersectCount
      } else 0)
    })

    val moviesDict = UtilityFunctions.getMoviesDict(moviesRefRDD.toDF().collect)
    val result = UtilityFunctions.getTopMUsers(mConfBroadcast.value, UConfBroadcast.value, userReducerRDD.collect, moviesDict)
    UtilityFunctions.printResult(result)
  }
}


