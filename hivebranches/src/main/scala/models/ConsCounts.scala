 package models
 import java.sql.ResultSet

 case class ConsCounts(beverage: String, counts: Int){
    override def toString(): String = {
        f"$beverage%-10s $counts%-10d"
    }
 }

 //companion object
 object ConsCounts{
    def apply(): ConsCounts = { objectifyResultSet()}

    def objectifyResultSet(rs : ResultSet): ConsCounts ={
    //use the generated case class apply that the companion object also gets to create an instance
    val countsObj =  apply(
                        rs.getString(1),
                        rs.getInt(2),
                      )
    countsObj
    }

    def objectifyResultSet(beverage: String = "", counts: Int = 0) = {
        apply(beverage,counts)
    }

    def getColumnHeader(): String = {

        "\nBeverage" + " " *20  + "Counts"  
    }

    def printColumnHeader(): Unit = {
        println(getColumnHeader())
        println("-"*35)
    }
 }