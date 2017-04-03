/**
  * Created by kiranmeda on 2/10/17.
  */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object filter_rdd {
  def main (args:Array[String]) {
//val conf= new SparkConf().setAppName("filter_RDD").setMaster("local")
  //  new SparkContext(conf)

val spk = SparkSession.builder().master("local[*]").appName("filters").getOrCreate()
    val sc = spk.sparkContext

// val data = Array(1,2,3,4) //val reduce = distdata.filter(x=>(x/2==0))

    val wrds=List("is","was","was","is","hive","hadoop","hive")
    val wrdsRdd = sc.parallelize(wrds)
    val filtered_rdd=wrdsRdd.filter(x=>x!="hive" && x!="was")
    val cntRdd = wrdsRdd.map(x=>(x,1))
    val reduceRdd = cntRdd.reduceByKey((x,y)=>(x+y))
    val sort=reduceRdd.sortBy(_._2,false).collect // display values by descending
    val topVals=sort.take(4)  // take top 4


  }

}
