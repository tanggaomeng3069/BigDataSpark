package cn.edu360.shangguigu.lihaibo.spark.core.framework.controller

import cn.edu360.shangguigu.lihaibo.spark.core.framework.common.TController
import cn.edu360.shangguigu.lihaibo.spark.core.framework.service.WordCountService

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:16
  * Describe: 控制层
  */
class WordCountController extends TController{

    private val wordCountService = new WordCountService()

    // TODO 调度
    def dispatch(): Unit = {

        // 执行业务操作
        val array: Array[(String, Int)] = wordCountService.dataAnalysis()
        array.foreach(println)
    }
}
