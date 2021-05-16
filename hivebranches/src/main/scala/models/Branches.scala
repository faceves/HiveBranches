package models

import java.sql.ResultSet
/**
sealed trait CoffeeShop {
    val beverage: String
    def objectifyResultSet
}

case class Base[T,U](x: T, y: U) {
    def toString(): String = {
        f"$x%-15 \t $y%-8"
    }
}


object Base{
    def objectifyResultSet(rs: ResultSet): Base[T,U] = [
        if(U.getClass.getSimpleName == "Int")
            
    ]
}
**/
case class  Branches(beverage : String, branch : String){

    override def toString(): String = {
        f"$beverage%-10s $branch%-10s"
    }
}

object Branches{
    def apply(): Branches = { objectifyResultSet()}

    def objectifyResultSet(rs : ResultSet): Branches = {
    //use the generated case class apply that the companion object also gets to create an instance
    val branchesObj =  apply(
                        rs.getString(1),
                        rs.getString(2),
                      )
    branchesObj
    }

    def objectifyResultSet(beverage: String = "", branch: String = "") = {
        apply(beverage,branch)
    }

    def getColumnHeader(): String = {

        "\nBeverage" + " " *20  + "Branch"  
    }

    def printColumnHeader(): Unit = {
        println(getColumnHeader())
        println("-"*35)
    }
}