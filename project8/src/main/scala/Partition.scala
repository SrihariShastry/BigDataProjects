import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, _}
import org.apache.spark.rdd.RDD

object Partition {
  def main ( args: Array[String] ) {
    val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var count:Int = 1

    //Constructing RDD of Edges
    val edges:RDD[Edge[Long]] = sc.textFile(args(0)).map(line=>{
      val point = line.split(",")
      (point(0).toLong,point.drop(1).toList.map(_.toLong))
    }).flatMap{case (node, adj) => adj.map{ p => Edge(node,p,p) } } //srcAttr,dstAttr,attr

    //Accessing vertices and assigning -1 to all except first 5
    val graph:Graph[Long,Long] = Graph.fromEdges(edges,"defaultValue").mapVertices((vertexID,_)=>
    if(count<=5){count = count+1;vertexID}else -1.toLong)

    // GraphX Pregel Job
    val preg=graph.pregel( -1.toLong,5)(
      (_,centroid,newCentroid)=>  if(centroid<0) math.max(newCentroid,centroid) else centroid,
      triplet=>Iterator((triplet.dstId,triplet.srcId)),
      (a,b)=>math.min(a,b)
    )
    //  Printing the number of values per centroid
    preg.vertices.map(g=>(g._2,1)).countByKey().foreach(l=>println(l._1,l._2))
  }
}
