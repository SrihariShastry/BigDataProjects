import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {
  val depth = 8

  def main ( args: Array[ String ] ):Unit = {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var count:Int = 1
    val depth:Int = 8
    var graphRDD = sc.textFile(args(0)).map(line => {
      val point = line.split(",")
      var centroid:Long= -1
      if (count <= 5) {
        centroid = point(0).toLong
      } else {
        centroid = -1
      }
      count = count + 1
      (point(0).toLong, centroid, point.drop(1).toList.map(_.toLong))
    })

    for(i<- 1 to depth){
      graphRDD = graphRDD.flatMap { case (node, centorid, adj) => (node, centorid) :: adj.map{ p => (p, centorid) } }
        .reduceByKey(_ max _)
        .join(graphRDD.map(g=>(g._1,(g))))
        .map({case (id,(newN,adjacent))=> {
          var centroid:Long= -1
          if(adjacent._2 < 0){
            centroid = newN

          }
          else
            centroid = adjacent._2
          (id,centroid,adjacent._3)
        }
        })
    }
    var graphPartition = graphRDD.map(g=>(g._2,1)).countByKey()
    graphPartition.foreach(l=>println(l._1,l._2))
  }
}
