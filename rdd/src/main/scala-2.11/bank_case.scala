/**
  * Created by kiranmeda on 2/14/17.
  */
/*import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object bank_case {
  val spark=SparkSession.builder().master("").appName("").getOrCreate()
  val sc=spark.sparkContext

  case class client(age:Int,marital:String,bal:Int)
  val bank=sc.textFile("file:///home/ubuntu/bank.csv")
  val heder=bank.first()
  val brdd=bank.filter(x=>x!=heder).map(x=>x.split(";")).map(x=>client(x(0).toInt,x(2).replaceAll(";",""),x(5).toInt))
  brdd.toDF().registerTempTable("k")

}
case class sales(Transaction_date:String,Product:String,Price:Int,Payment_Type:String,Name:String,City:String,State:String,Country:String,Account_Created:String,Last_Login:String, Latitude:String,Longitude:String)
val df=sc.textFile("file:///home/ubuntu/Sales.csv")
val heder=df.first()
val srdd=df.filter(x=>x!=heder).map(x=>x.replaceAll("(.*),(.*)","$1 $2")).map(x=>x.split(",")).map(x=>sales(x(0),x(1),x(2).toInt,x(3),x(4),x(5),x(6),x(7),x(8),x(9),x(10),x(11)))
*/