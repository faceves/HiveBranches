package dao

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Seq
import utils.ConnectionUtil
import scala.util.Using
import java.sql.ResultSet
import scala.util.{Try, Success, Failure}
import java.sql.Connection
import models.Branches


object BranchesDAO {

  def getTotalConsumers(branch: String):Try[String] = {
    var conn: Connection = null
    Using.Manager{use=>
      conn = use(ConnectionUtil.getConnection())
      val stmnt = use(conn.prepareStatement(
        "Select SUM(c.qty) as total " +
          "FROM branches b FULL OUTER JOIN " +
          "Counts c ON (b.beverage = c.beverage) " +
          "WHERE b.Branch = ?"))
      stmnt.setString(1,branch)
      stmnt.execute
      val rs = stmnt.getResultSet()
      rs.next()
      //return counts
      rs.getString(1)
    }
  }

  def getLeastOrMostBeverage(branch: String, order: String): Try[Tuple2[String,Int]] = {
    var conn: Connection = null
    Using.Manager{use=> 
      conn = use(ConnectionUtil.getConnection())
      val stmnt = use(conn.prepareStatement(
        "SELECT b.beverage, SUM(c.qty) as total " +
        "FROM branches b " +
        "FULL OUTER JOIN " +
        "Counts c ON (b.beverage = c.beverage) " +
        "WHERE b.Branch = ? " +
        "GROUP BY b.beverage " +
        s"ORDER BY total $order " +
        "LIMIT 1"))
      stmnt.setString(1,branch)
      stmnt.execute
      val rs = use(stmnt.getResultSet())
      rs.next()
      //return Tuple of least or most beverage consumed
      (rs.getString(1),rs.getInt(2))
    }
  }

  def getListBeverages(branchList: Seq[String], queryFlag: String): Try[ArrayBuffer[String]] = {
    var conn: Connection = null
    var query: String = ""
    if(queryFlag == "common"){
      query = "SELECT DISTINCT b.beverage " +
          "FROM branches b " +
          "WHERE b.branch = ? AND b.beverage IN " +
          "(SELECT a.Beverage FROM branches a WHERE a.branch = ?) " +
          "ORDER BY b.beverage"
    }
    else{
      query = "Select DISTINCT Beverage " +
          "FROM Branches " +
          "WHERE Branch = ? OR Branch = ? OR Branch = ?"
    }

    Using.Manager{use=> 
      conn = use(ConnectionUtil.getConnection())
      val stmnt = use(conn.prepareStatement(query))

      for(x <- 0 to branchList.size - 1){
        stmnt.setString(x+1,branchList(x))
      }
      stmnt.execute

      val rs = use(stmnt.getResultSet())
      var bevList = ArrayBuffer[String]()
      while(rs.next()){
        bevList += rs.getString(1)
      }
      //return list of beverages
      bevList
      
    }

  }

  /**
  def getAvailableBeverages(branchList: Seq[String]): Try[ArrayBuffer[String]] = {
    var conn: Connection = null
    Using.Manager{use=> 
      conn = use(ConnectionUtil.getConnection())
      val stmnt = use(conn.prepareStatement(
        
        ))

      for(x <- 0 to branchList.size - 1){
        stmnt.setString(x+1,branchList[x])
      }
      stmnt.execute

      val rs = use(stmnt.getResultSet())
      var bevList = ArrayBuffer[String]()
      while(rs.next()){
        bevList += rs.getString(1)
      }
      //return list of beverages
      bevList
      
    }
  }
  **/
}
