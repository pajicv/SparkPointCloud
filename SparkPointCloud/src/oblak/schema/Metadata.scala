package oblak.schema

private[schema] case class Offsets(var x: Double, var y: Double, var z: Double);

private[schema] case class ScaleFactors(var x: Double, var y: Double, var z: Double);

case class Metadata(name: String, 
                    /*
                     * Number of bits to shift point morton code in order to produce block morton code.
                     * eg. if we have mm precision of point coordinates (0.001m) then with resolution of 3
                     * one block will be cube with side dimension of 0.008m. (4 => 0.016m, 5 => 0.032m).
                     * With resolution 0 size of block will be equal to precision of coordinates.  
                     */
                    resolution: Int, 
                    extent: BoundingBox,
                    offsets: Offsets,
                    scaleFactors: ScaleFactors) {
  
}