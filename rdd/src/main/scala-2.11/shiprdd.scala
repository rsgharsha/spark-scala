/**
  * Created by kiranmeda on 2/14/17.
  */


//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SQLContext

object shiprdd {

  def main(args: Array[String]) {
    //val conf= new SparkConf().setAppName("filter_RDD").setMaster("local")
    //  new SparkContext(conf)
    //val dataRdd = sc.textFile("/home/Titanic.csv")
    val spark = SparkSession.builder().master("local[*]").appName("filters").getOrCreate()
    import spark.implicits._
    val dataRdd = spark.read.textFile("/home/Titanic.csv")
    //val sc = spark.sparkContext
    //val dataRdd = sc.textFile("/home/Titanic.csv")


    val headerRdd = dataRdd.first()
    //val toDF = dataRdd.filter(x=>x!=(dataRdd.first())).map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6))).toDF()
    val datFrame=dataRdd.filter(x=>x!=headerRdd).map(x=>(x.split(",")(0),x.split(",")(1),x.split(",")(2),x.split(",")(3),x.split(",")(4),x.split(",")(5),x.split(",")(6))).toDF("slno","name","pclss","age","sex","survived","sexcd")
    datFrame.createOrReplaceTempView("ship")
    spark.sql("select * from ship limit 4").collect.foreach(println)
    spark.sql("select * from ship").coalesce(1).write.format("csv").save("")


  }
//  object shiprdd {

  }
