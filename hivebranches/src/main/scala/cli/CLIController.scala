package cli

import dao.BranchesDAO
import scala.util.{Try,Success,Failure}

object CLIController {

    def totConsumersFromBranch(branch: String): Unit = {
        val consTotTry: Try[String] = BranchesDAO.getTotConsumersFromBranch(branch)
        consTotTry match{
            case Success(consumerTotal) => {
                println("Consumer Total: ")
                println("-"*10)
                println(s"$consumerTotal")
            }
            case Failure(e) => println(e.getMessage())
        }
    }

    def leastOrMostBevFromBranch(branch: String, symbol: String) : Unit= {
        //check for correct symbol
        var ordering = "ASC";
        if(symbol == "<")
            ordering = "DESC"
        else if(symbol != ">"){
            println("Incorrect Symbol to distinguish least or most beverage.")
            return
        }
        val beverageTry:Try[Tuple2[String,Int]] = BranchesDAO.getLeastOrMostBevFromBranch(branch, ordering)
        beverageTry match{
            case Success((beverage,count)) => {
                println("Beverage \t\t Count")
                println("-"* 25)
                println(f"$beverage%-10s \t $count%-10d")
            }
            case Failure(e) => println(e.getMessage())

        }
    }
}
