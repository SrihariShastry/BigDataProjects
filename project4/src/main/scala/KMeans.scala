import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double, Double)

  var centroids: Array[Point] = Array[Point]()

  def main(args: Array[String]) {
    /* ... */
    val conf = new SparkConf().setAppName("Join")
    val sc = new SparkContext(conf)

    var centroidsRDD = sc.textFile(args(1)).map(line => {
      val a = line.split(",")
      new Point(a(0).toDouble, a(1).toDouble)
    }) /* read initial centroids from centroids.txt */

    //reading points
    val points = sc.textFile(args(0)).map(line => {
      val a = line.split(",")
      new Point(a(0).toDouble, a(1).toDouble)
    })

    for (i <- 1 to 5) {
      centroids = centroidsRDD.collect()
      val cs = sc.broadcast(centroids)
      val newCentroidsRDD = points.map { p => (cs.value.minBy(distance(p, _)), p) }
        .groupByKey().map(t => {
        var sumX = 0.000000
        var sumY = 0.000000
        for (p <- t._2) {
          sumX = sumX + p._1
          sumY = sumY + p._2
        }
        val cX = sumX/t._2.size
        val cY = sumY/t._2.size
        new Point(cX,cY)
      })
      centroidsRDD=newCentroidsRDD
    }
    centroids = centroidsRDD.collect()
    centroids.foreach(println)
  }

  def distance(p1: Point, p2: Point): Double = {
    Math.sqrt(Math.pow(p1._1 - p2._1, 2) + Math.pow(p1._2 - p2._2, 2))
  }
}
