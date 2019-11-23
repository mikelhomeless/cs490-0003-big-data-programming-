import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.spark._

object FacebookFriends {
  Logger.getLogger("org.apache.spark").setLevel(Level.WARN)

  def mapFriends(input: String) = {
    val splitInput = input.split("->")
    val user = splitInput(0)
    val friendList = splitInput(1)
    val friends = friendList.split(",")

    val results :List[String] = List.empty
    friends.map { friend =>
      if (user.charAt(0) < friend.charAt(0)){
        results.+(user + "," + friend)
      } else {
        results.+(friend + "," + user)
      }
    }
    results.map(result => (result, friends))
  }

  def reduceFriends(kvpair: (String, String)) = {
    val set1 = new util.HashSet[String]
    val list = kvpair._2.split(",")

    list.foreach { friend :String =>
      val temp = friend.split(",")
      if (set1.isEmpty) {
        for (value <- temp){
          set1.add(value)
        }
      } else {
        val set2 = new util.HashSet[String]
        for (value <- temp){
          set2.add(value)
        }
        set1.retainAll(set2)
      }
    }
    val finalList = set1.toArray()
    val sb = StringBuilder.newBuilder
    finalList.foreach { case string :String =>
        sb.append(string + ",")

    }
    (kvpair._1 + sb.toString())
  }

  def main(args: Array[String]) {

//    System.setProperty("hadoop.home.dir","C:\\winutils" )

    val conf = new SparkConf().setAppName("Facebook").setMaster("local[*]")

    val sc = new SparkContext(conf)
    // Load our input data.

    val input = sc.textFile("facebook.txt")
    // Split up into words.
    val users = input.flatMap(mapFriends)

//    val output = users.reduceByKey(reduceFriends)
//    output.saveAsTextFile("output")
  }
}