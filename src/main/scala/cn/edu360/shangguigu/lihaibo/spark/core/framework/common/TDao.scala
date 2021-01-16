package cn.edu360.shangguigu.lihaibo.spark.core.framework.common

import cn.edu360.shangguigu.lihaibo.spark.core.framework.util.EnvUtil
import org.apache.spark.rdd.RDD

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 15:09
  * Describe: 三层架构中的：数据访问的对象
  */
trait TDao {

    def readFile(path: String): RDD[String] = {
        EnvUtil.take().textFile(path)
    }

}
