package com.stark.study

object study2 {
  def main(args: Array[String]): Unit = {
    var test1 :study2 = new study2()

    var c : Int = test1.add(1,2)
    var d : Int = test1.multiply(4,5)
  }
}


/**
 * 函数与方法异同
 * 方法是类的一部分           (方法是组成类的一部分。)
 * 函数是一个对象可以赋值给一个变量 (函数则是一个完整的对象)
 */
class  study2{
  // 函数示例
  val add: (Int, Int) => Int = { (a, b) => a + b}

  // 方法示例
  def multiply(a: Int, b: Int): Int = {
    var c: Int = a + b
    println("这是一个方法")
    return c
  }

  // 调用函数
  private val result1: Int = add(3, 5)
  println(result1) // 输出: 8

  // 调用方法
  private val result2 = multiply(4, 6)
  println(result2) // 输出: 24
}



