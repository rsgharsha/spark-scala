/**
  * Created by swapna on 2/14/17.
  */
import org.apache.spark.sql.{SQLContext, SparkSession}

object usingdfapi {
  val spark=SparkSession.builder().master("local[*]").appName("filters").getOrCreate()
  //val sqlContext=new SQLContext(sc)
  val df=spark.read.format("com.databricks.spark.csv").option("delimiter",";").load("file:///home/ubuntu/bank.csv")
  df.createOrReplaceTempView("bank")
  spark.sql("select * from bank")


}
