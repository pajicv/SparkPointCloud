package oblak.udf

import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
 
class CollectPointsInArray extends UserDefinedAggregateFunction {
  def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("mortonCode", LongType) :: Nil)
 
  def bufferSchema: StructType = StructType(
    StructField("mortonCodes", DataTypes.createArrayType(LongType, false)) :: Nil
  )
 
  def dataType: DataType = DataTypes.createArrayType(LongType, false)
 
  def deterministic: Boolean = true
 
  def initialize(buffer: MutableAggregationBuffer): Unit = { 
    buffer(0) = Array.emptyLongArray
  }
 
  def update(buffer: MutableAggregationBuffer,input: Row): Unit = {
    
    val points = buffer.getSeq(0)
    val point = input.getAs[Long](0)
    
    buffer(0) = points ++ Seq(point)
  }
 
  def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    
    val points1 = buffer1.getSeq(0)
    val points2 = buffer2.getSeq(0)
    
    buffer1(0) = points1 ++ points2
  }
 
  def evaluate(buffer: Row): Any = {
    buffer(0)
  }
}