package dao

import scala.collection.mutable.ArrayBuffer
import utils.ConnectionUtil
import scala.util.Using
import java.sql.ResultSet
import scala.util.{Try, Success, Failure}
import java.sql.Connection
import models.Branches


object BranchesDAO {

  def getTotConsumersFromBranch(branch: String):Try[String] = {
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

  def getLeastOrMostBevFromBranch(branch: String, order: String): Try[Tuple2[String,Int]] = {
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
        "ORDER BY total ? " +
        "LIMIT 1"))
      stmnt.setString(1,branch)
      stmnt.setString(2,order)
      stmnt.execute
      val rs = use(stmnt.getResultSet())
      //return Tuple of least or most beverage consumed
      (rs.getString(1),rs.getInt(2))
    }
  }
}
