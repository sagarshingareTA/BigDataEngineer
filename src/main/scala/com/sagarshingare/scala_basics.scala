package com.sagarshingare

//Another way we can run scala app as web app
//object Main extends  App {
//}


object scala_basics {

  def sumInt(firstValue:Int,secondValue:Int): Unit = {
    val sum=firstValue+secondValue
    print("First Value: "+firstValue + " Second Value: "+ secondValue + " Sum :" +sum)
  }

  def main(args: Array[String]): Unit = {
    //Functional call
    val a = 12
    val b = 123
    sumInt(a,b)

    //defining a value
    val meaningOfLife: Int = 42 //cont int meaningOfLife =42

    //Int , Boolean, Char, Double, Float, String

  }
}






///use later based on concept

/*

//TIP Press <shortcut actionId="ShowIntentionActions"/> with your caret at the highlighted text
    // to see how IntelliJ IDEA suggests fixing it.
    (1 to 5).map(println)

    for (i <- 1 to 5) {
      //TIP Press <shortcut actionId="Debug"/> to start debugging your code. We have set one <icon src="AllIcons.Debugger.Db_set_breakpoint"/> breakpoint
      // for you, but you can always add more by pressing <shortcut actionId="ToggleLineBreakpoint"/>.
      println(s"i = $i")
 */

