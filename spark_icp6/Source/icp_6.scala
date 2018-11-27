
import org.apache.spark.sql.SparkSession
import org.graphframes.GraphFrame

object icp_6 {
  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()
    val input = spark.read.format("csv").option("header", "true").load("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\Spark_icp_6\\station_data.csv")

    //input.show()
    val input1 = input.select("name","landmark","dockcount").withColumnRenamed("name","id")
    //input1.show()

    val output = spark.read.format("csv").option("header", "true").load("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\Spark_icp_6\\trip_data.csv")
    //output.show()
    val output1 = output.select("Start_Station","End_Station","Duration").withColumnRenamed("Start_Station","src").withColumnRenamed("End_Station","dst").withColumnRenamed("Duration","relationship")
    //output1.show()



    val g = GraphFrame(input1,output1)
    //g.vertices.show()
    //g.edges.show()

    val results = g.triangleCount.run()
    //results.show()
    //results.select("id", "count").show()

    val sp = g.shortestPaths.landmarks(Seq("Japantown","Cowper at University")).run()
    //sp.show()

    val pr = g.pageRank.resetProbability(0.15).tol(0.01).run()
    //pr.vertices.select("id", "pagerank").show()
    //pr.edges.select("src", "dst", "weight").show()



    //g.vertices.write.csv("data/ver")
    //g.edges.write.csv("data/edge")

// bonus part
    val lpa = g.labelPropagation.maxIter(5).run()
    //lpa.show()
    //lpa.select("id", "label").show()

    val BFS = g.bfs.fromExpr("id = 'Japantown'").toExpr("dockcount<424").run()
    BFS.show()


  }
}
