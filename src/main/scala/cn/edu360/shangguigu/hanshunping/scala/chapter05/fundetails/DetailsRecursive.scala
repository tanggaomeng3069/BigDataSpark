package cn.edu360.shangguigu.hanshunping.scala.chapter05.fundetails

object DetailsRecursive {
  def main(args: Array[String]): Unit = {

  }

  def f1(n:Int): Int = {
      if (n==1) {
        1
      }else {
        f1(n-1)
      }
  }
}
