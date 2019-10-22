package co.ogury.segmentation

object ActivitySegment {
  sealed abstract class ActivitySegment(label: String){
    override def toString: String = label
  }
  final case object ACTIVE extends ActivitySegment("active")
  final case object INACTIVE extends ActivitySegment("inactive")
  final case object NEW extends ActivitySegment("new")
  final case object UNDEFINED extends ActivitySegment("undefined")

  def fromString(value: String): Option[ActivitySegment] = Set(ACTIVE, INACTIVE, NEW, UNDEFINED).find(_.toString == value)
}
