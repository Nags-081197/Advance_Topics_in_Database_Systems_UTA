import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object KMeans {
  type Point = (Double,Double)

  var centroids: Array[Point] = Array[Point]()

  def distance (p: Point, point: Point): Double ={
    var distance = Math.sqrt ((p._1 - point._1) * (p._1 - point._1) + (p._2 - point._2) * (p._2 - point._2));
    distance
  }

  def main(args: Array[ String ]): Unit = {
    val conf = new SparkConf().setAppName("KMeans")
    val sc = new SparkContext(conf)

    centroids = sc.textFile(args(1)).map( line => { val ln1 = line.split(",") 
    (ln1(0).toDouble,ln1(1).toDouble)}).collect
    var points=sc.textFile(args(0)).map(line=>{val ln2=line.split(",") 
    (ln2(0).toDouble,ln2(1).toDouble)})

    for ( i <- 1 to 5 ){
      val cs = sc.broadcast(centroids)
      centroids = points.map { p => (cs.value.minBy(distance(p,_)), p) }
        .groupByKey().map { case(c,pointva)=>
        var cnt=0
        var sumofx=0.0
        var sumofy=0.0

        for(ps <- pointva) {
           cnt += 1
           sumofx+=ps._1
           sumofy+=ps._2
        }
        
        var centroid_x=sumofx/cnt
        var centroid_y=sumofy/cnt
        (centroid_x,centroid_y)

      }.collect
    }
  centroids.foreach(println)
  
  }
}
