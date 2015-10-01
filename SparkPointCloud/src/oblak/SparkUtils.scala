package oblak

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object SparkUtils {
 
  	val conf = new SparkConf().setAppName("SparkPointCloud").setMaster("local")
  
  	val spark = new SparkContext(conf)
  	
  	val sql = new SQLContext(spark)
  	
}

