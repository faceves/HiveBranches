package dao
import scala.util.{Try, Success, Failure}
import java.sql.Connection
import utils.ConnectionUtil
import scala.util.Using
import scala.collection.mutable.ArrayBuffer

trait HiveBaseDAO {
    /**
    def createTable()
    def loadTable()
    def ctas()
    def dropTable()
    def createView()
    def dropView()
    def createIndex()
    def dropIndex() **/
    def useDatabase(dbName: String) : Try[Boolean]
}

object HiveDAO extends HiveBaseDAO{

    def useDatabase(dbName: String): Try[Boolean] = {
        var conn: Connection = null
        Using.Manager{use=>
            conn = use(ConnectionUtil.getConnection())
            val stmnt = use(conn.prepareStatement("use ?"))
            stmnt.setString(1,dbName)
            stmnt.execute()
        }
    }

    def showDB(): Try[ArrayBuffer[String]] = {
      var conn: Connection = null;
      Using.Manager{ use =>
        conn = use(ConnectionUtil.getConnection())

        val stmnt = use(conn.prepareStatement("show tables"))
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
