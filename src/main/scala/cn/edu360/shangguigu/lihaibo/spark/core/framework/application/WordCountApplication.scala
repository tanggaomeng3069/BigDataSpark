package cn.edu360.shangguigu.lihaibo.spark.core.framework.application

import cn.edu360.shangguigu.lihaibo.spark.core.framework.common.TApplication
import cn.edu360.shangguigu.lihaibo.spark.core.framework.controller.WordCountController

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:14
  * Describe:
  * 1.extends APP之后就用写main函数，就能执行伴生对象
  */
object WordCountApplication extends App with TApplication {

    // TODO 启动程序
    start(){
        val wordCountController = new WordCountController()
        wordCountController.dispatch()
    }

}
