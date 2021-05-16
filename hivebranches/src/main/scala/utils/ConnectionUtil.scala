package utils


import java.sql.Connection;
import java.sql.DriverManager;

object ConnectionUtil {
  
    // recommended: environment variables and connectin pool to make 5 and only 5 and wait tilll one opens
    var conn: Connection = null
    //environment variables for database login
    val dbUsernameEnv = ""
    val dbPasswordEnv = ""
    val dbConnectionPort = "jdbc:hive2://localhost:10000/"
    val dbName = "project1"
    val driverName = "org.apache.hive.jdbc.HiveDriver"


    /** utility for retrieving connection
        *
        * This should properly be a connection pool.  Instead, we'll use it
        * like a connection pool with a single connection, that gets returned
        * whenever any part of our application needs it
        *
        * @return Connection
        */
    def getConnection(): Connection = {
        var conn:Connection = null
        
        // if conn is null or closed, initialize it
        if (conn == null || conn.isClosed()) {
            Class.forName(driverName)

            //grab connection for database through JDBC Java Drivermanager
            conn = 
                DriverManager.getConnection(
                dbConnectionPort.concat(dbName), 
                getDBLoginInfo("username"),
                getDBLoginInfo("password") 
                )
        }
        // return conn, potentially after initialization
        conn
    }

    private def getDBLoginInfo(loginCredential:String): String = {
        var loginInfo : Option[String] = None
        if(loginCredential.equals("username"))
            loginInfo = sys.env.get(dbUsernameEnv)
        else
            loginInfo = sys.env.get(dbPasswordEnv)
    
        loginInfo match{
            case Some(s) => {
                s
            }
            case None => { 
                if(loginCredential.equals("username"))
                    println("No username found.")
                else
                    println("No password found.") 
                ""
            }
        }    
    }

    def connectionCheck(): Boolean = {
        var conn:Connection = null
        try{
            conn = getConnection()
            if(conn==null)
                false
            else
                true
        }
        catch{
            case e: Exception => {
                println(e.getMessage())
                false
            }
        }
        finally{
            if(conn != null)
                conn.close()
        }
    }

    
}
