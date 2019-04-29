// Databricks notebook source
// MAGIC %md ### (Homework #2) - Scala - (your name)

// COMMAND ----------

// MAGIC %md #### 1) Write a function to validate whether parentheses are balanced
// MAGIC 
// MAGIC Expecting true for the following strings
// MAGIC 
// MAGIC <ul>
// MAGIC   <li>(if (zero? x) max (/ 1 x))</li>
// MAGIC   <li>I told him (that it’s not (yet) done). (But he wasn’t listening)</li>
// MAGIC </ul>  
// MAGIC   
// MAGIC Expecting false for the following strings
// MAGIC <ul>
// MAGIC   <li>:-)</li>
// MAGIC   <li>())(</li>
// MAGIC  </ul>
// MAGIC 
// MAGIC 
// MAGIC The following methods are useful for this challenge
// MAGIC chars.isEmpty
// MAGIC chars.head
// MAGIC chars.tail
// MAGIC Hint: you can define an inner function if you need to pass extra parameters to your function.
// MAGIC 
// MAGIC To convert a String to List[Char] ==> "ucsc school".toList
// MAGIC 
// MAGIC Extra credit: write another implementation that uses pattern match with list extraction pattern

// COMMAND ----------

import scala.collection.mutable.Stack
var stack = Stack[Char]()
def balance(chars: List[Char]): Boolean = {
  for (i <- 0 until chars.length){
     if(chars(i)=='('){
        stack.push(chars(i))
     }
      else if(chars(i)==')'){
       if(stack.isEmpty) return false
       stack.pop()
     }
  }
  if(stack.isEmpty)
        return true
  else return false
}
val list = "(if (zero? x) max (/ 1 x))".toList
println(balance(list))


//testcase1: "(if (zero? x) max (/ 1 x))"  output:true
//testcase2: "())("  output:false
//testcase2: ":-)"  output:false
//TIME COMPLEXITY O(N) N is length of the string
//SPACE COMPLEXITY O(N) N is the size of the stack

// COMMAND ----------

// MAGIC %md #### 2) Write two functions for performing run length encoding and decoding:
// MAGIC 
// MAGIC <p>encoding("WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWW") ==> "12W1B12W3B8W"</p>
// MAGIC <p>decoding("12W1B12W3B8W") ==> "WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWW"</p>

// COMMAND ----------

def encoding(input:String) : String = {
  val chars = input.toList
  var length =chars.length
  var text="";
  var i=0
  while(i<chars.length){
      var count=1
      while (i < length - 1 &&  
          chars(i) == chars(i + 1)) { 
          count =count+1 
          i=i+1 
       } 
     text = text + count+chars(i) 
     i=i+1
  }
  text
}
encoding("WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWW")
//TIME COMPLEXITY O(N) N is length of the string
//SPACE COMPLEXITY O(N) I is the length of the char array
//TESTCASE1:input:""  output:""
//TESTCASE2:input:"WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWW"  output:"12W1B12W3B8W"


// COMMAND ----------

def decoding(input:String) : String = {
  val chars = input.toList
  var length =chars.length
  var text="";
  var i=0
  var beginindex=0;
  while(i<chars.length){
    if(chars(i).isLetter){
      val digit=input.slice(beginindex,i).toInt
      for (j <- 0 until digit){
          text=text+chars(i)
      }
      beginindex=i+1
    }
    i=i+1
  }
  text
}

print(decoding("12W1B12W3B8W"))

//TESTCASE1:input:"12W1B12W3B8W"  output:"WWWWWWWWWWWWBWWWWWWWWWWWWBBBWWWWWWWW"

// COMMAND ----------

// MAGIC %md #### 3) Convert decimal value roman numeral

// COMMAND ----------

// Mapping between decimal to 
val romanNumeralMap = Map(1000 -> "M", 900 -> "CM", 500 -> "D", 400 -> "CD", 
                          100 -> "C", 90 -> "XC", 50 -> "L", 40 -> "XL", 
                          10 -> "X", 9 -> "IX", 5 -> "V", 4 -> "IV", 1 -> "I")


// COMMAND ----------

import scala.collection.mutable.ListBuffer

case class RomanNumeral(number: Int) {
  val DecimalsToNumerals = collection.mutable.LinkedHashMap(romanNumeralMap.toSeq.sortWith(_._1 > _._1):_*).toList
  private def toRoman = DecimalsToNumerals.foldLeft((List.empty[String], number)){
    case ((roman, remaining), (decimal, numeral)) =>
      val result = remaining/decimal
      (roman ++ List.fill(result)(numeral), remaining - decimal * result)
  }
  val value = toRoman._1.mkString
}

def decimalToRomanNumeral(n:Int) : String = {
  return new RomanNumeral(n).value 
}
println(decimalToRomanNumeral(1997))

//TESTCASE1:input:"1997"  output:"MCMXCVII"

// COMMAND ----------

// decimalToRomanNumeral(1997) => expecting MCMXCVII

// https://www.rapidtables.com/convert/number/roman-numerals-converter.html

// COMMAND ----------

case class RomanNumeral(number: Int) {
  val DecimalsToNumerals = collection.mutable.LinkedHashMap(romanNumeralMap.toSeq.sortWith(_._1 > _._1):_*).toList
  private def toRoman = DecimalsToNumerals.foldLeft((List.empty[String], number)){
    case ((roman, remaining), (decimal, numeral)) =>
      val result = remaining/decimal
      (roman ++ List.fill(result)(numeral), remaining - decimal * result)
  }
  val value = toRoman._1.mkString
}
val m1 = new RomanNumeral( 1997)
println(m1.value)

