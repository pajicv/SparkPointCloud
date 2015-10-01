package oblak.schema

/*
 * Šema oblaka tačaka koja će se koristiti za kreiranje DataFrame.
 */
case class PointCloud(var id: Int,
                      var name: String,
                      extent: BoundingBox,
                      blocks: Block) {
  
}