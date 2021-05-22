import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {

  val depth = 6

  def main ( args: Array[ String ] ): Unit = {
  	val conf = new SparkConf().setAppName("Partition")
    val sc = new SparkContext(conf)
    var i = 0
    var graph = sc.textFile(args(0)).map(line=>{val lineval = line.split(",")
    	i = i+1
	    if (i<=5) {
	    	(lineval(0).toLong,lineval(0).toLong,lineval.drop(1).toList.map(_.toString).map(_.toLong))
	    }else {
	    	(lineval(0).toLong,-1.toLong, lineval.drop(1).toList.map(_.toString).map(_.toLong))
	    } 
	})

	for (i <- 1 to depth) {
		graph = graph.flatMap( map => map match{ case (x, y, xy) => (x, y) :: xy.map(z => (z, y) ) } )
                .reduceByKey(_ max _)
                .join( graph.map( gg=>(gg._1, (gg._2, gg._3)) ) )
                .map( g => {
                      if(g._2._2._1 == -1){(g._1,g._2._1,g._2._2._2)}
                      else{(g._1,g._2._2._1,g._2._2._2)}
                    } )
	}

	val totalGroupCount = graph.map(g => (g._2, 1))
	.reduceByKey((k1, k2) => (k1 + k2))
	.collect()
	.foreach(println)
  }
}