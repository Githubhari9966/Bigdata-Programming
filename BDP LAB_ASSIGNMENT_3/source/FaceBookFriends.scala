import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

object FaceBookFriends {

  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)

    /*val sc1 = SparkSession
    .builder
    .appName("SparkWordCount")
    .master("local[*]")
    .getOrCreate().sparkContext*/

    val frd = sc.textFile("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3\\facebook_combined.txt")

    val flist = frd.map(line => line.split(" ").toList)
    val f1 = flist.map(x => (x(0), x(1)))
    val f2 = flist.map(x => (x(1), x(0)))
    val f3 = f1.union(f2)
    val f4 = f3.groupBy(_._1).mapValues(_ map (_._2))

    f4.filter(_._2.size > 1)

    val common = f4.flatMap { x =>

      val x1 = x._1
      val flist = x._2.toList
      val friendslist = flist.sorted

      //friendslist.foreach(println)
      val makelist = friendslist.slice(0, friendslist.size).map(y => {
        if (x1 > y) (y, x1) else (x1, y)
      })
      makelist.map(z => (z, friendslist.slice(0, friendslist.size).toSet))
    }
    val commonfriends = common.reduceByKey((x, y) => x intersect y).sortByKey()
    commonfriends.collect().take(10).foreach(x => {
    println(s"${x._1} -> (${x._2.mkString(" ")})")
    commonfriends.saveAsTextFile("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3")
    })

  }

  }
