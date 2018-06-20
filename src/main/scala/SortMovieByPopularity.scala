import org.apache.spark.SparkContext
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD

object SortMovieByPopularity
{
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("Movie By Popularity");
    val sc   = new SparkContext(conf);


    val csvSplitExpr = ",(?=([^\"]*\"[^\"]*\")*(?![^\"]*\"))";

    // Get the contents of the movies.csv file.
    val movieHdr  = "movieId,title,genres"
    val moviesAll = sc.textFile(args(0)).filter(_ != movieHdr)
    val nbrMovies = moviesAll.count

    // Get the contents of the ratings.csv file.
    val ratingsHdr = "userId,movieId,rating,timestamp"
    val ratingsAll = sc.textFile(args(1)).filter(_ != ratingsHdr)
    val nbrRatings = ratingsAll.count


    val movieIdInput = moviesAll.map(_.split(csvSplitExpr)).map(x => (x(0).toInt,x(1)));

    val ratingUserId = ratingsAll.map(_.split(","))
      .map(x => (x(1).toInt,1)) // Assign count of 1 to each occurrence of MovieID
      .reduceByKey((x,y) => (x + y)) // Compute # ratings per MovieID


    //println(ratingUserId.count()+"------------"+movieIdInput.count())
   val result =movieIdInput.join(ratingUserId)



    val out=result.map(data => {
      val key = data._2._1
      val vals = data._2._2
      (vals,key)
      }).sortBy(_._1,false)

   out.saveAsTextFile(args(2));

    }

}
