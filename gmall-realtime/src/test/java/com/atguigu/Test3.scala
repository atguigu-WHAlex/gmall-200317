package com.atguigu

import java.time.LocalDate

object Test3 {

  def main(args: Array[String]): Unit = {

    val today: String = LocalDate.now().toString
    println(today)
  }

}
