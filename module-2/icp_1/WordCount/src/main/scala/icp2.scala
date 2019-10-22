import org.apache.spark._
import java.io.{BufferedWriter, FileWriter}

object Icp2 {
  type Vertex = Int
  type Graph = Map[Vertex, List[Vertex]]
  val g: Graph = Map(1 -> List(2,3,5,6,7), 2 -> List(1,3,4,6,7), 3 -> List(1,2), 4 -> List(2,5,7),5 -> List(1,6,7),6 -> List(1,2,5,7),7 -> List(1,2,4,5,6))

  def merge(l1:List[Int], l2:List[Int]): List[Int] = (l1,l2) match{
    case(Nil, _) => l2
    case(_, Nil) => l1 // returns the one that is not nil
    case (h1::t1, h2::t2) => // assumed sorted list
      if(h1<h2) h1:: merge(t1,l2) // if head is bigger than second head then merge first then second otherwise other
      else h2::merge(l1,t2)
  }

  def dfs(start: Vertex, g: Graph): List[Vertex] = {

    def DFS_recur(v: Vertex, visited: List[Vertex]): List[Vertex] = {
      if (visited.contains(v))
        visited
      else {
        val neighbours:List[Vertex] = g(v) filterNot visited.contains
        neighbours.foldLeft(v :: visited)((b,a) => DFS_recur(a,b))
      }
    }
    DFS_recur(start,List()).reverse
  }

  def main(args: Array[String]) {
    //System.setProperty("hadoop.home.dir","F:\\winutils" )
    val conf = new SparkConf().setAppName("Icp2").setMaster("local[*]")
    // Create a Scala Spark Context.
    val sc = new SparkContext(conf)
    val listToSort = sc.parallelize(List[Int](2,4,9,5,26,2,0,8))
    val resultList = listToSort.map(sub_l => List[Int](sub_l))
    val merged = resultList.reduce((l1, l2) => merge(l1, l2))
    val file = "icp1_output.txt"
    val writter = new BufferedWriter(new FileWriter(file))
    writter.write(merged.toString() + "\n")

    val dfsResult = dfs(1, g)
    writter.write("dfsresult: " + dfsResult.toString())
    writter.close()
  }
}
