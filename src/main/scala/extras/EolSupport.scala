package extras

object EolSupport {

  /**
   *
   * @param text A String where JSON objects are separated by commas, and where each JSON object may have
   *             several end of line characters
   * @return A String where each line marks the end of a JSON object
   */
  def removeEOL(text: String): String = {
    val splitWithIndex = text.split("\n").zipWithIndex

    def shouldKeepEOL(line: String, index: Int): Boolean = {
      index < splitWithIndex.size -2 && line.trim == "}," && splitWithIndex(index + 1)._1.trim == "{"
    }

    val indicesOfEolsToRetain = splitWithIndex.foldLeft( Vector.empty[Int]  ){ (vec, next) =>
      next match {
        case (line, index) => if(shouldKeepEOL(line, index)) {
          vec :+ index
        } else vec
      }

    }.toSet

    splitWithIndex.foldLeft(new StringBuilder){ (sb, lineAndIndex) =>
      lineAndIndex match {
        case (line, index) =>
          if (indicesOfEolsToRetain.contains(index)) {
            sb.append(line + "\n")
          } else {
            sb.append(line)
          }
          sb
      }

    }.toString()

  }

}
