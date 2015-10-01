package oblak.schema

/**
 * @author Jelena
 */
case class Block(mortonCode: Long,
                 extent: BoundingBox,
                 points: Array[Point]) {
  
}