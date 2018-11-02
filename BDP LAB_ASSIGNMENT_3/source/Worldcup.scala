import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._

object Worldcup {

  def main(args: Array[String]) {
    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local")
      .getOrCreate()


    val df1 = spark.read.format("csv").option("header", "true").load("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3\\WorldCupMatches.csv")
    //df1.show()
    val df2 = spark.read.format("csv").option("header", "true").load("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3\\worldcups.csv")

    //df2.show()

    // q1(ok)
   //df1.select("Stadium").show()
    //q2:(ok)
    //df1.groupBy("City").count().show(5)

    //q3(ok): sql query on dataframe
    //df1.createOrReplaceTempView("WorldCupMatches")

   // val sqlDF1 = spark.sql("SELECT Stage, Stadium, City from WorldCupMatches")
    //sqlDF1.show(20)

    //df2.createOrReplaceTempView("worldcups")
    //val sqlDF2 = spark.sql("SELECT year, Country, Winner FROM worldcups")
    //sqlDF2.show()
    //val unionDF = sqlDF1.union(sqlDF2)
    //unionDF.show()



    // Register the DataFrame as a global temporary view

    //q4(ok)
    df1.createGlobalTempView("WorldCupMatches")
    // Global temporary view is tied to a system preserved database `global_temp`
    //spark.sql("SELECT Stage, Stadium, City, Attendance, MatchID FROM global_temp.WorldCupMatches").show(20)


    //q5

    val y = spark.sql("SELECT Year, Country, GoalsScored From worldcups WHERE GoalsScored>100")
    //y.show()

    //q6:
    val Pattern_reg = spark.sql("SELECT * from WorldCupMatches WHERE Stage LIKE 'Final' AND Home_Team_Initials LIKE 'ENG'")
    //Pattern_reg.show()

   //q7:
   val Avg_goals = spark.sql("SELECT Home_Team_Name AS Team, ROUND(AVG(Home_Team_Goals),0) AS average_goals FROM WorldCupMatches GROUP BY Home_TEam_Name")
    //Avg_goals.show()

   //q8:



    //RDD

    //val RDD = spark.sparkContext.textFile("C:\\Users\\veliv\\Desktop\\BigData Programming Fall18\\LAB ASSIGNMENT3\\WorldCupPlayers_t")
    val Goals = spark.sql("SELECT Stage,Stadium,City,Home_Team_Name FROM WorldCupMatches WHERE Home_Team_Goals >6")
    Goals.show()


  }
}
