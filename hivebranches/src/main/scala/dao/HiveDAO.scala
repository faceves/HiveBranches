package dao
import scala.util.{Try, Success, Failure}
import java.sql.Connection
import java.sql.ResultSet
import utils.ConnectionUtil
import scala.util.Using
import scala.collection.mutable.ArrayBuffer


object HiveDAO{

    def createTable(tblName: String, columnList : String, partitionedBy: String = "", 
                    clusteredBy: String = "", location: String = ""): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"CREATE TABLE IF NOT EXISTS $tblName " +
                  s"($columnList) " +
                  s"$partitionedBy " +
                  s"$clusteredBy " +
                  "ROWS FORMAT DELIMITED " +
                  "FIELDS TERMINATED BY \",\" " +
                  "STORED AS TEXTFILE" +
                  s"$location "
                ))
            stmnt.execute()
        }
    }

    def loadTable(tblName: String, pathfile: String, overwriteFlag: Boolean = false, 
                    localFlag: Boolean = false): Try[Boolean] = {
        var conn: Connection = null
        var overwrite = ""
        var local = ""
        if(!overwriteFlag)
            overwrite = "OVERWRITE"
        if(!localFlag)
            local = "LOCAL"
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"LOAD DATA $local INPATH '$pathfile' $overwrite INTO TABLE $tblName"
                ))
            stmnt.execute()
        }
    }

    def ctas(tblName: String, query: String):Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"CREATE TABLE $tblName IF NOT EXISTS AS" +
                  s"$query "
                ))
            stmnt.execute()
        }
    }

    def dropTable(tblName: String) : Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(s"DROP TABLE $tblName "))
            stmnt.execute()
        }
    }

    def showTables(): Try[ArrayBuffer[String]] = {
      var conn: Connection = null;
      Using.Manager{ use =>
        conn = use(ConnectionUtil.getConnection())

        val stmnt = use(conn.prepareStatement("show tables"))
        stmnt.execute
        val rs = stmnt.getResultSet()
        var tblList: ArrayBuffer[String] = ArrayBuffer[String]()
        while(rs.next()){
            tblList.+=( rs.getString(1))
        }
        tblList
      }
    }

    def createView(viewName : String, query: String): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"CREATE VIEW IF NOT EXISTS $viewName AS $query"
                ))
            stmnt.execute()
        }
    }

    def dropView(viewName: String) : Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"DROP VIEW $viewName"
                ))
            stmnt.execute()
        }
    }

    def showViews() : Try[ArrayBuffer[String]] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"show views"
                ))
            stmnt.execute()
            val rs = stmnt.getResultSet()
            var viewList: ArrayBuffer[String] = ArrayBuffer[String]()
            while(rs.next()){
                viewList.+=( rs.getString(1))
            }
            viewList
        }
    }

    def createIndex(indexName: String, tblName : String, indexedCol: String): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"CREATE INDEX $indexName " +
                  s"ON TABLE $tblName($indexedCol) " +
                  s"AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' " +
                  s"WITH DEFERRED REBUILD"
                ))
            stmnt.execute()
            val alterStmnt = use(conn.prepareStatement(
                s"ALTER INDEX $indexName ON $tblName REBUILD"
            ))
            alterStmnt.execute()
        }
    }

    def dropIndex(indexName: String, tblName:String): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"DROP INDEX $indexName ON $tblName"
                ))
            stmnt.execute()
        }
    }

    def showIndexes(tblName: String): Try[ArrayBuffer[String]] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(
                s"show index ON $tblName"
                ))
            stmnt.execute()
            val rs = stmnt.getResultSet()
            var indexList: ArrayBuffer[String] = ArrayBuffer[String]()
            while(rs.next()){
                indexList.+=( rs.getString(1))
            }
            indexList
        }
    }

    def useDatabase(dbName: String): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement(s"use $dbName"))
            stmnt.execute()
        }
    }



    def showDB(): Try[ArrayBuffer[String]] = {
      var conn: Connection = null;
      Using.Manager{ use =>
        conn = use(ConnectionUtil.getConnection())

        val stmnt = use(conn.prepareStatement("show databases"))
        stmnt.execute
        val rs = stmnt.getResultSet()
        var dbList: ArrayBuffer[String] = ArrayBuffer[String]()
        while(rs.next()){
            dbList.+=( rs.getString(1))
        }
        dbList
      }
  }
}
