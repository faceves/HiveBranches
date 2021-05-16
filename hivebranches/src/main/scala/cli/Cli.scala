package cli;

import scala.io.StdIn
import scala.util.matching.Regex
import scala.util.{Try,Success,Failure}
import utils.ConnectionUtil
import dao.HiveDAO

object Cli {

    private val commandPattern: Regex =  "(\\w+)\\s*.*".r
  
    def run(){
    
        initialSetup()
        menu()

    }

    private def menu(){
        var contMenuLoop = true

        printGreeting()
        do{
            printMenuOptions()

            var userInput:String = StdIn.readLine() //blocking
            
            userInput match {
                case "1"  => {
                    scn1Menu()                    
                }
                case "2"  => {
                    scn2Menu()                    
                }
                case "3"  => {
                    scn3Menu()                    
                }
                case "4"  => {
                    scn4Menu()                  
                }
                case "5"  => {
                    scn5Menu()                   
                }
                case "6"  => {
                    scn6Menu()                   
                }
                case commandPattern(cmd) if cmd == "exit" => {
                    contMenuLoop = false
                }
                case commandPattern(cmd) => {
                    println(s"Parsed command $cmd did not correspond to an option")
                }
                case _ => {
                    println("Failed to parse command.")
                }
            }

        }while(contMenuLoop)
    }

    private def printGreeting():Unit = {
        println()
        println("*"*60)
        println("\t\tWelcome to the HiveApp!")
        println("*"*60)
    }

    private def printMenuOptions():Unit = {
        List(
            "\nMenu Options:",
            "-------------",
            "1)         Scenario 1",
            "2)         Scenario 2",
            "3)         Scenario 3",
            "4)         Scenario 4",
            "5)         Scenario 5",
            "6)         Scenario 6",
            "exit)      Exits the app.\n"
            ).foreach(println)
    }

    /**
      * Problem Scenario 1 
What is the total number of consumers for Branch1?
What is the number of consumers for the Branch2?



Problem Scenario 2 
What is the most consumed beverage on Branch1
What is the least consumed beverage on Branch2

Problem Scenario 3
What are the beverages available on Branch10, Branch8, and Branch1?
what are the comman beverages available in Branch4,Branch7?

Problem Scenario 4
create a partition,index,View for the scenario3.

Problem Scenario 5
Alter the table properties to add "note","comment"

Problem Scenario 6
Remove the row 5 from the output of Scenario 1 
      */
    private def scn1Menu() = {
        println("Please choose the following problem given the number:")
        println("1) What is the total number of consumers for Branch1?")
        println("2) What is the number of consumers for the Branch2?")
        var userDone = false
        var userInput = ""

        do{
            userInput = StdIn.readLine().trim()
            userInput match{
                case "1" =>{
                    println("Processing query!\nPlease wait ...")
                    CLIController.totConsumersFromBranch("Branch1")
                    userDone = true;
                }
                case "2" =>{
                    println("Processing query!\nPlease wait ...")
                    CLIController.totConsumersFromBranch("Branch2")
                    userDone = true;
                }
                case "exit" => userDone = true
                case _ => println("Incorrect command. Please enter 1, 2, or exit.")
            }
        }while(!userDone)
        
    }

    private def scn2Menu() = {
        println("Please choose the following problem given the number:")
        println("1) What is the most consumed beverage on Branch1?")
        println("2) What is the least consumed beverage on Branch2?")
    }

    private def scn3Menu() = {
        println("Please choose the following problem given the number:")
        println("1) What are the beverages available on Branch10, Branch8, and Branch1?")
        println("2) What are the comman beverages available in Branch4, Branch7?")

        
    }

    //create a partition, index, View for the scenario3.
    private def scn4Menu() = {
        println("Creating a partition, views, and index's for Scenario 3")
    }

    private def scn5Menu() = {
        println("Add a comment/note in Table Properities:")

        HiveDAO.showDB() match {
            case Success(value) => value.foreach(println)
            case Failure(e) => println(e.getMessage())
        }
    }

    private def scn6Menu() = {
        println("Remove row 5 from table in Scenario 3:")
    }

    private def initialSetup():Unit = {
        if(!ConnectionUtil.connectionCheck()){
            println("Database connection not established, please retry again.")
            System.exit(-1)
        }
         
         HiveDAO.useDatabase("franciscodb") match{
            case Success(bool) => { 
                if(bool) println("Success!") 
                else println("Fail!")
            }
            case Failure(e) => println(e.getMessage())
        }
    }

}
