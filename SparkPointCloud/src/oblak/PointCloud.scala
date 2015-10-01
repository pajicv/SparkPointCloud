package oblak

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructType,StructField,LongType,ArrayType};
import java.io._

import oblak.schema._
import oblak.sfc._
import oblak.udf.CollectPointsInArray

object PointCloud {
  
  def load(path: String): DataFrame = {
    
    val points = SparkUtils.spark.textFile(path).map(_.split(" ")).map {fields => {
        val coordinates = Coordinates(fields(0).trim().toDouble,
                                 fields(1).trim().toDouble,
                                 fields(2).trim().toDouble)  
                                 
        val normalizedCoordinates = NormalizedCoordinates(0, 0, 0)
                          
        Point(coordinates,
              normalizedCoordinates,
              0,
              0,
              fields(3).trim().toInt,
              0,
              fields(4).trim().toShort,
              fields(5).trim().toShort,
              fields(6).trim().toShort)
        } 
    }
    
    import SparkUtils.sql.implicits._
    points.toDF()
    
  }
  
  def findExtent(points: DataFrame): BoundingBox = {
    val stats: Row = points.agg(min(points.col("coordinates.x")),
                                min(points.col("coordinates.y")),
                                min(points.col("coordinates.z")),
                                max(points.col("coordinates.x")),
                                max(points.col("coordinates.y")),
                                max(points.col("coordinates.z")))
                           .collect().apply(0)

                                 
    val minCoords = Coordinates(stats.getDouble(0), stats.getDouble(1), stats.getDouble(2))
    val maxCoords = Coordinates(stats.getDouble(3), stats.getDouble(4), stats.getDouble(5))
    
    
    
    BoundingBox(minCoords,
                maxCoords,
                NormalizedCoordinates(0, 0, 0),
                NormalizedCoordinates(0, 0, 0))
    
  }
  
  def normalizeBoundingBox(bbox: BoundingBox, precision: Double): BoundingBox = {
    val f = (1 / precision).toInt
    BoundingBox(bbox.min,
                bbox.max,
                bbox.normalizedMin, //it is already 0,0,0
                NormalizedCoordinates((bbox.max.x - bbox.min.x).toInt * f,
                                      (bbox.max.y - bbox.min.y).toInt * f,
                                      (bbox.max.z - bbox.min.z).toInt * f)
                )
  }
  
  def normalizeCoordinates(points: DataFrame, 
                           bbox: BoundingBox, 
                           precision: Double,
                           resolution: Int): DataFrame = {
        
    import SparkUtils.sql.implicits._
    
    
    var pointsBeforeNormalization = points.select("coordinates.x", "coordinates.y", "coordinates.z", 
        "mortonCode", "blockMortonCode",
        "intensity", "classification", 
        "red", "green", "blue")
    
        
    pointsBeforeNormalization = pointsBeforeNormalization.withColumn("precision", lit((1 / precision).toInt))
                                                         .withColumn("minX", lit(bbox.min.x))
                                                         .withColumn("minY", lit(bbox.min.y))
                                                         .withColumn("minZ", lit(bbox.min.z))                                            
    
    println("Precision: %d".format((1 / precision).toInt))
    println("Min coords: %f, %f, %f".format(bbox.min.x, bbox.min.y, bbox.min.z))
    
    var pointsAfterNormalization = pointsBeforeNormalization.selectExpr("x", 
                                                                        "y", 
                                                                        "z",
                                                                        "round((x - minX) * precision) AS normX",
                                                                        "round((y - minY) * precision) AS normY",
                                                                        "round((z - minZ) * precision) AS normZ",
                                                                        "mortonCode", 
                                                                        "blockMortonCode",
                                                                        "intensity", 
                                                                        "classification",
                                                                        "red", 
                                                                        "green", 
                                                                        "blue")
                                                                      
    
    pointsAfterNormalization = pointsAfterNormalization.withColumn("resolution", lit(resolution * 3))
 
    SparkUtils.sql.udf.register("calculateMortonCode", (x: Double, y: Double, z: Double) => Z3.apply(x.toInt, y.toInt, z.toInt).z)
    
    var pointsWithMortonCodes = pointsAfterNormalization.selectExpr("x", 
                                                                    "y", 
                                                                    "z",
                                                                    "normX",
                                                                    "normY",
                                                                    "normZ",
                                                                    "calculateMortonCode(normX, normY, normZ) AS mortonCode", 
                                                                    "blockMortonCode",
                                                                    "intensity", 
                                                                    "classification",
                                                                    "red", 
                                                                    "green", 
                                                                    "blue",
                                                                    "resolution")
                                                        .orderBy("mortonCode")
                                                                    
    SparkUtils.sql.udf.register("calculateBlockMortonCode", (mc: Long, r: Int) => mc >>> r)
                                                                    
    var pointsWithBlockMortonCodes = pointsWithMortonCodes.selectExpr("x",
                                                                      "y",
                                                                      "z",
                                                                      "mortonCode",
                                                                      "calculateBlockMortonCode(mortonCode, resolution) AS blockMortonCode",
                                                                      "intensity", 
                                                                      "classification",
                                                                      "red", 
                                                                      "green", 
                                                                      "blue")
                                                          
                                                                      
    pointsWithBlockMortonCodes//.orderBy("blockMortonCode")
    
    /*val collectPoints = new CollectPointsInArray                                                               
    SparkUtils.sql.udf.register("collectPoints", collectPoints)
    
    var blocks = pointsWithBlockMortonCodes.groupBy("blockMortonCode")
                                           .agg(expr("collectPoints(mortonCode) AS mortonCodes"))
                                                                      
    blocks*/
            
  }
  
  def clip(blocks: DataFrame, extent: BoundingBox, resolution: Int, precision: Double, clipBox: BoundingBox): DataFrame = {
    val f: Int = (1 / precision).toInt
    
    val minX: Int = ((clipBox.min.x - extent.min.x) * f).toInt
    val minY: Int = ((clipBox.min.y - extent.min.y) * f).toInt 
    val minZ: Int = ((clipBox.min.z - extent.min.z) * f).toInt 
    val maxX: Int = ((clipBox.max.x - extent.min.x) * f).toInt
    val maxY: Int = ((clipBox.max.y - extent.min.y) * f).toInt 
    val maxZ: Int = ((clipBox.max.z - extent.min.z) * f).toInt
    
    val minMortonCode = Z3.apply(Z3.apply(minX, minY, minZ).z >>> (resolution * 3))
    val maxMortonCode = Z3.apply(Z3.apply(maxX, maxY, maxZ).z >>> (resolution * 3))
    
    println("Min. block morton code: ", minMortonCode.z)
    println("Max. block morton code: ", maxMortonCode.z)
    
    val mortonCodeRanges = Z3.zranges(minMortonCode, maxMortonCode)
    
    var condition = ""
    
    mortonCodeRanges.foreach(range => {
      condition += "( blockMortonCode BETWEEN " + range._1 + " AND " + range._2 + ") "
      if(range != mortonCodeRanges.last) {
        condition += " OR "
      }
    })
    
    println(condition)
    
    blocks.registerTempTable("blocks")
    
    SparkUtils.sql.sql("SELECT blockMortonCode, points FROM blocks WHERE " + condition)
    
  }
  
  
  def collectPointsInBlocks(points: DataFrame): DataFrame = {
    
    var pointRDD: RDD[(Long, Point)] = points.map { 
      case Row(x: Double, 
               y: Double, 
               z: Double, 
               mortonCode: Long, 
               blockMortonCode: Long,
               intensity: Int,
               classification: Byte,
               red: Short,
               green: Short,
               blue: Short) => {
                 (blockMortonCode, Point(Coordinates(x, y, z),
                                         NormalizedCoordinates(0,0,0),
                                         mortonCode,
                                         blockMortonCode,
                                         intensity,
                                         classification,
                                         red, 
                                         green, 
                                         blue))
               }
    }
    
    val blocks = pointRDD.groupBy(point => point._1)
                          .map(block => {
                            val result = block._2.map(record => record._2)
                            (block._1, result.toSeq)
                          })
    
    
    import SparkUtils.sql.implicits._
             
    val df = blocks.toDF("blockMortonCode", "points")
    
    df.orderBy("blockMortonCode")
    
  } 
  
  def ingest() {

    val points = load("pc.pts")
    
    println("Loaded points")
    points.printSchema()
    points.show()
    
    val bbox = findExtent(points)
    val normBbox = normalizeBoundingBox(bbox, Constants.PRECISION)
    val normalizedPoints = normalizeCoordinates(points, normBbox, Constants.PRECISION, Constants.RESOLUTION)
    
    println("Normalized points")
    normalizedPoints.show(5)
    
    val blocks = collectPointsInBlocks(normalizedPoints)

    println("Blocks")
    blocks.printSchema()
    blocks.show(5)
        
    blocks.write.format("parquet").save("pointcloud2.parquet")
    
    blocks.write.format("json").save("pointcloud2.json")
    
  }
  
  def read(): DataFrame = {
    
    SparkUtils.sql.read.parquet("pointcloud2.parquet")
    
  }
  
  
   
  def main(args: Array[String]) {
    
    val blocks = read()
    
    val normBbox = BoundingBox(Coordinates(4.025, -15.3, -1.75),
                               Coordinates(16.375, -4.169, 9.2),
                               NormalizedCoordinates(0, 0, 0),
                               NormalizedCoordinates(0, 0, 0))
    
    val selectedBlocks = clip(blocks,
                              normBbox,
                              Constants.RESOLUTION,
                              Constants.PRECISION, 
                              BoundingBox(Coordinates(7.0, -13.0, -1.5),
                                          Coordinates(8.5, -12.0, -0.5),
                                          NormalizedCoordinates(0, 0, 0),
                                          NormalizedCoordinates(0, 0, 0)))
                                          
   selectedBlocks.printSchema()
    
   val rdd = selectedBlocks.map { 

      case Row(bmc: Long, points: Seq[Row]) => (bmc, points)
      
   }
    
   rdd.collect().foreach(record => {
     
     val points = record._2
     
     points.foreach { row => {
       val coordinates = row.getAs[Row](0)
       val x = coordinates.getDouble(0)
       val y = coordinates.getDouble(1)
       val z = coordinates.getDouble(2)
       
       println("%.3f %.3f %.3f".format(x, y, z))
     }} 
     
   })
   
    
    /*val scaleFactorX = sc.broadcast(0.001);
    val scaleFactorY = sc.broadcast(0.001);
    val scaleFactorZ = sc.broadcast(0.001);
    
    
    val scaledPoints = points.map {p =>
      DataFramePoint((p.x * (1 / scaleFactorX.value)).toInt,
                   (p.y * (1 / scaleFactorY.value)).toInt,
                   (p.z * (1 / scaleFactorZ.value)).toInt,
                   1, //unclassified
                   p.intensity,
                   p.red,
                   p.green,
                   p.blue,
                   0,
                   0)
      
    }*/
  
    //scaledPoints.persist()
    
    /*var dataFrame = scaledPoints.toDF()
    
    var stats = dataFrame.describe("x", "y", "z");
    
    stats.printSchema()
    
    var collectedStats = stats.collect
    
    val minRow = collectedStats(3)
    val maxRow = collectedStats(4)
    
    val minX = minRow(1).asInstanceOf[String].toInt;
    val minY = minRow(2).asInstanceOf[String].toInt;
    val minZ = minRow(3).asInstanceOf[String].toInt;
    val maxX = maxRow(1).asInstanceOf[String].toInt;
    val maxY = maxRow(2).asInstanceOf[String].toInt;
    val maxZ = maxRow(3).asInstanceOf[String].toInt;
                                  
    println(minX + ", " + minY + ", " + minZ)
    println(maxX + ", " + maxY + ", " + maxZ)
    
    val bbox = sc.broadcast(Array(minX, minY, minZ, maxX, maxY, maxZ))
    
    val normalizedPoints = scaledPoints.map { p => 
      val p1 = p
      
      p1.x = p1.x - bbox.value(0)
      p1.y = p1.y - bbox.value(1)
      p1.z = p1.z - bbox.value(2)
      
      val z3 = Z3.apply(p1.x, p1.y, p1.z)
      p1.mortonCode = z3.z
      p1.blockMortonCode = p1.mortonCode >>> 24
      
      p1
    }
    
    var pointsDataFrame = normalizedPoints.toDF()
    
    pointsDataFrame.registerTempTable("points")
    
    val octree = sqlContext.sql("SELECT blockMortonCode, collect_set(mortonCode) AS nump FROM"
      + " (SELECT blockMortonCode, mortonCode FROM points ORDER BY blockMortonCode, mortonCode) AS a "
      + "GROUP BY blockMortonCode ORDER BY nump DESC")
      
    octree.show()*/
    
    //pointsDataFrame = pointsDataFrame.sort("blockMortonCode","mortonCode")
    
    //var groupedPointsDataFrame = pointsDataFrame.groupBy("blockMortonCode")
    
    /*val groupedPoints = normalizedPoints.groupBy(p => p.mortonCode)
    
    groupedPoints.persist()
    
    val rddFunctions = new PairRDDFunctions(groupedPoints)
    
    val numOfPointsPerGroup = rddFunctions.countByKey()
    
    val numOfGroups = groupedPoints.count()*/
    
    
    /*val clipMinPointX = (7.0 * 1000).toInt - minX
    val clipMinPointY = (-13.5 * 1000).toInt - minY
    val clipMinPointZ = (-1.5 * 1000).toInt - minZ*/
    
    /*val clipMaxPointX = (8.5 * 1000).toInt - minX
    val clipMaxPointY = (-12.0 * 1000).toInt - minY
    val clipMaxPointZ = (-0.5 * 1000).toInt - minZ*/
    
    /*val z3min = Z3.apply(clipMinPointX - 50, clipMin/PointY - 50, clipMinPointZ - 50)
    val z3max = Z3.apply(clipMinPointX + 50, clipMinPointY + 50, clipMinPointZ + 50)*/
    
    //dataFrame = normalizedPoints.toDF()
    
    //val stats2 = dataFrame.describe("mortonCode")
    
    //stats2.show()
    
    //dataFrame.orderBy("mortonCode")
    
    /*val ranges = Z3.zranges(z3min, z3max)
    
    var condition = ""
    
    ranges.foreach(range => {
      condition += "( mortonCode BETWEEN " + range._1 + " AND " + range._2 + ") "
      if(range != ranges.last) {
        condition += " OR "
      }
    })
    
    println(condition)
    
    dataFrame.registerTempTable("points")*/
    
    //udf 
    /*val inRadius = udf((p1: DataFramePoint, p2: DataFramePoint) => {
      ( Math.pow((p2.x - p1.x), 2) + Math.pow((p2.y - p1.y), 2) +  Math.pow((p2.z - p1.z), 2) ) <= 100
    })*/
    
    /*dataFrame = sqlContext.sql("SELECT p1.mortonCode AS current, p2.mortonCode AS nearPoint " + 
        " FROM points p1 INNER JOIN points p2 ON " +
        " ((p2.x - p1.x)*(p2.x - p1.x) + (p2.y - p1.y)*(p2.y - p1.y) + (p2.z - p1.z)*(p2.z - p1.z)) <= 100")
    
        
    val pointsInRadius = dataFrame.flatMap(r => (r(0), r(1))*/
    
    /*val t1 = System.currentTimeMillis()
    
    dataFrame = sqlContext.sql("SELECT x, y, z, mortonCode FROM points WHERE " + condition)
    
    val t2 = System.currentTimeMillis()
    
    dataFrame.show()*/
    
    /*dataFrame.write
             .mode("overwrite")
             .format("parquet")
             .partitionBy("mortonCode")
             .save("points.parquet");*/
    
    //dataFrame.registerTempTable("points")
    
    //count points in octant
    /*val countByMorton = dataFrame.groupBy("mortonCode").count() 
    
    val numOfOctants = countByMorton.count() //count octants
    
    println("Broj oktanata je %d".format(numOfOctants))
    
    countByMorton.show();
    
    val stats1 = countByMorton.describe("count")
    
    stats1.show()*/
    
    

    //println("Processing time %d sec".format((t2 - t1) / 1000))
    
    
    
    //clipedPoints.write.mode("overwrite").save("~/spark_apps/SparkPointCloud/clipped.csv")    
    
  }                 
  
  /*def toPoint(fields: Array[String]): InputPoint = {
    return InputPoint(fields(0).trim().toDouble,
               fields(1).trim().toDouble,
               fields(2).trim().toDouble,
               fields(3).trim().toByte,
               fields(4).trim().toShort,
               fields(5).trim().toShort,
               fields(6).trim().toShort)
  }
                   
  def preprocessPoint(p: InputPoint): DataFramePoint = {    
    return DataFramePoint((p.x * (1 / this.metadata.scaleFactor.x)).toInt,
                   (p.y * (1 / this.metadata.scaleFactor.y)).toInt,
                   (p.z * (1 / this.metadata.scaleFactor.z)).toInt,
                   1, //unclassified
                   p.intensity,
                   p.red,
                   p.green,
                   p.blue)
  }*/
    
}