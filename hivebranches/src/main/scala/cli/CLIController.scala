package cli

import dao.BranchesDAO
import dao.HiveDAO
import scala.util.{Try,Success,Failure}
import scala.collection.mutable.Seq
import scala.collection.mutable.ArrayBuffer

object CLIController {

    def displayTotalConsumers(branch: String): Unit = {
        val consTotTry: Try[String] = BranchesDAO.getTotalConsumers(branch)
        consTotTry match{
            case Success(consumerTotal) => {
                println("Consumer Total: ")
                println("-"*10)
                println(s"$consumerTotal")
            }
            case Failure(e) => println(e.getMessage())
        }
    }

    def displayLeastOrMostBeverage(branch: String, problemNum: String) : Unit= {
        //check for correct symbol
        var ordering = "ASC";
        if(problemNum == "1")
            ordering = "DESC"
        val beverageTry:Try[Tuple2[String,Int]] = BranchesDAO.getLeastOrMostBeverage(branch, ordering)
        beverageTry match{
            case Success((beverage,count)) => {
                println("Beverage \t Count")
                println("-"* 25)
                println(f"$beverage%-10s \t $count%-10d")
            }
            case Failure(e) => println(e.getMessage())

        }
    }

    def displayBeverageList(branchList: Seq[String], queryFlag : String): Unit = {
        val bevListTry: Try[ArrayBuffer[String]] = BranchesDAO.getListBeverages(branchList,queryFlag)
        bevListTry match{
            case Success(bevList) => {
                println("Beverage: ")
                println("-"*10)
                bevList.foreach(println)
            }
            case Failure(e) => println(e.getMessage())
        }
    }

    def displayScn3Optimizaiton(){
        //table variables
        val tableName = "branchespart"
        val colList = "beverage String, branch String"
        val partitionedBy = "PARTITIONED BY(branch)"
        //view variables
        val viewName1 = "branch10_8_1"
        val query1 = "" +
          "SELECT DISTINCT beverage " +
          "FROM branchespart " +
          "WHERE branch = 'Branch10' OR branch = 'Branch8' OR branch = 'Branch1'"
        val viewName2 = "common_bev4_7"
        val query2 = "" +
          "SELECT DISTINCT b.beverage " +
          "FROM branches b " +
          "WHERE b.branch = 'Branch4' AND b.beverage IN " +
          "(SELECT a.Beverage FROM branches a WHERE a.branch = 'Branch7') " +
          "ORDER BY b.beverage"
        //index variables
        val indexName1 = "idx_bevbr"
        val idxTblName1 = "branchespart"
        val indexedCol1 = "beverage"
        val indexName2 = "idx_bevcounts"
        val idxTblName2 = "counts"
        val indexedCol2 = ""
        
        
        //creating partitioned table:
        displayCreateTable(tableName,colList,partitonedBy)

        //creating views
        displayView(viewName1, query1)
        displayView(viewName2, query2)

        //creating index
        displayCreateIndex(indexName1, idxTblName1, indexedCol1)
        displayCreateIndex(indexName2, idxTblName2, indexedCol2)

    }
    
    //helper
    def displayCreateTable(tableName : String, colList: String, partitionedBy: String = "", 
                            clusteredBy : String = "", location: String =""): Unit = {
        val createdTry: Try[Boolean] = HiveDAO.createTable(tableName,colList,partitionedBy,clusteredBy,location)
        createdTry match{
            case Success(created) =>{
                if(created)
                    println("Successfully created table: " + tableName)
                else
                    println("Unsuccesfuly created the table: " + tableName)
            }
            case Failure(e) => println(e.getMessage())
        }
    }

    //helper
    def displayCreateView(viewName : String, query: String): Unit = {
        val createdTry = HiveDAO.createView(viewName,query)
        createdTry match{
            case Success(created) =>{
                if(created)
                    println("Succesfully created view: " + viewName)
                else
                    println("Unsuccesfully created view: " + viewName)
            }
            case Failure(e) => println(e.getMessage())
        }
    }

    //helper
    def displayCreateIndex(indexName : String, tblName: String, indexedCol: String): Unit = {
        val createdTry = HiveDAO.createIndex(indexName, tblName, indexedCol)
        createdTry match{
            case Success(created) =>{
                if(created)
                    println(s"Succesfully created index: $indexName on $tblName" )
                else
                    println(s"Unsuccesfully created index: $indexName on $tblName")
            }
            case Failure(e) => println(e.getMessage())
        }
    }

}
