import scala.io.Source
import scala.util.Random
import java.io._

object file_bs {
  def main(args: Array[String]): Unit = {
    val file = Source.fromFile("lorem.txt")
    val lines = file.getLines.toList
    val random = new Random()
    1 to 30 foreach { i =>
      val bw = new BufferedWriter(new FileWriter(new File("log/log" + i + ".txt")))
      bw.write(lines.slice(random.nextInt(lines.length), lines.size).mkString("\n"))
      bw.close()
    }
  }
}
