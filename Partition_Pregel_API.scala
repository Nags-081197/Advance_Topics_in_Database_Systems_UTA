import org.apache.spark.graphx.{Edge,Graph,VertexId}
import org.apache.spark.graphx.util.GraphGenerators
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object Partition {
  def main(args: Array[String] ) {
    val conf = new SparkConf().setAppName("graph_partition")
    val sc = new SparkContext(conf)

    def get_all_edges(strings: Array[String],i:Long): List[(Long,Long,Long)] ={
      var adjacentNodes: List[(Long,Long,Long)] = List[(Long,Long,Long)]()
      for(j <- strings.indices){
        adjacentNodes = (strings(0).toLong,strings(j).toLong,i) :: adjacentNodes
      }
      adjacentNodes
    }

    var count:Long = 0
    val graph_edges = sc.textFile(args(0)).map(line => {
      val line_value = line.split(",")
      count += 1
      get_all_edges(line_value,count)
    }).flatMap(e =>{
      e.map(ed =>{
        Edge(ed._1,ed._2,ed._3)
      })
    })

    val graph = Graph.fromEdges(graph_edges,1L)
    val initial_start_cluster = graph.edges.filter(e => e.attr <= 5).map(e=>{e.srcId}).distinct().collect()
    val ic_context = sc.broadcast(initial_start_cluster)
    val new_graph = graph.mapVertices((id,_) => {
      if(ic_context.value.contains(id))
        id
      else
        -1L
    })

    val final_graph_result = new_graph.pregel(-1L,maxIterations = 6)(
      (_,prev_attr,new_attr)=> {
        if(prev_attr == -1L)
          new_attr
        else
          prev_attr
      },
      triplet => {
        Iterator((triplet.dstId,triplet.srcAttr))
      },
      (a1,a2)=> Math.max(a1,a2)
    )
    final_graph_result.vertices.map(c => (c._2,1))
      .reduceByKey(_+_).collect().foreach(println)
    sc.stop()
  }
}