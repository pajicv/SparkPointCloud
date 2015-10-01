package oblak.schema

case class Point (coordinates: Coordinates,
                  var normalizedCoordinates: NormalizedCoordinates, //transformed to integers due to morton code calculation
                  var mortonCode: Long, //calculated from normalized coords.
                  var blockMortonCode: Long,
                  intensity: Int,
                  classification: Byte,
                  red: Short,
                  green: Short,
                  blue: Short) {
  
}