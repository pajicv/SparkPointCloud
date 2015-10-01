package oblak.schema

case class BoundingBox(min: Coordinates, 
                       max: Coordinates,
                       var normalizedMin: NormalizedCoordinates, 
                       var normalizedMax: NormalizedCoordinates) extends Serializable {
  
}