import org.apache.spark.sql.SparkSession

var list=List("is","was","this","hadoop","hive","hive")
var rdd = sc.parallelize(list)
var filterRdd = rdd.filter(x=>x!="is" && x!="this")
var reduceRdd = filterRdd.map(x=>(x,1)).reduceByKey((x,y)=>(x+y)).sortBy(_._2,false) // filters rdd by value in desc
reduceRdd.saveAsTextFile("/user/venujustforu/results")
