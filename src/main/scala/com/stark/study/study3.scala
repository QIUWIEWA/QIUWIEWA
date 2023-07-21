package com.stark.study

import scala.collection.mutable.Set

//复杂数据类型的定义与使用
object study3 {
  def main(args: Array[String]): Unit = {
    val t1: study3 = new study3()
    t1.fun()
    t1.fun2()
    t1.fun3()
  }
}
class study3{

  def fun() :Unit ={
    //定义一个一维数组
    val strs: Array[String] = new Array[String](4)

    //调用一维数组的内容,给他赋值
    strs(0) = "s"
    strs(1) = "t"
    strs(2) = "r"

    println(strs.mkString(""))
  }

  def fun2(): Unit = {
    //定义一个二维数组
    val arr1 = Array.ofDim[String](3, 3)

    val strs: Array[String] =Array("s","t","r")

    arr1(2)=strs
    //调用一维数组的内容,给他赋值


    for (row <- arr1){
      for (i <- row){
        print(i + "\t")
      }
      println("")
    }
  }

  def fun3(): Unit = {
    //集合
    val mutableSet = Set(1, 2, 3)
    println(mutableSet.getClass.getName) // scala.collection.mutable.HashSet

    mutableSet.add(4)
    println(mutableSet)//Set(1, 2, 3, 4)

    mutableSet.remove(1)
    println(mutableSet)//Set(2, 3, 4)

    mutableSet += 5
    println(mutableSet)//Set(5, 2, 3, 4)

    mutableSet -= 2
    println(mutableSet) // Set(5, 3, 4)
  }

  def fun4() :Unit = {
    //迭代器
    val it = Iterator("Baidu", "Google", "Runoob", "Taobao")

    while (it.hasNext) {
      println(it.next())
    }
  }
}