package za.co.teststream

object createColStrings {

  def createComplexColStrings(noOfCols: Int, noOfUpdates: Int): (List[String], List[String]) = {

    val colRange = 1 to noOfCols

    val columns = colRange.foldLeft(List[String]())(
      (colStringList: List[String], i: Int) => {

        val updateRange = 1 to noOfUpdates

        val complexColumn = updateRange.foldLeft("")(
          (s, j) => {
            val baseString = if (j != noOfUpdates) s"CASE WHEN UPDATES[$j].curr_bal > 0 THEN 1 ELSE 0 END + "
            else s"CASE WHEN UPDATES[$j].curr_bal > 0 THEN 1 ELSE 0 END AS Col$i"
            s + baseString
          }
        )
        colStringList ::: List(complexColumn)

      }
    )

    val aggColumns = colRange.foldLeft(List[String]())(
      (aggColStringList: List[String], i: Int) => {

        val aggCol = s"SUM(Col$i) AS AggCol$i"
        aggColStringList ::: List(aggCol)

      }
    )

    (columns, aggColumns)
  }


}
