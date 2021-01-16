package cn.edu360.shangguigu.lihaibo.spark.core.framework.common

import cn.edu360.shangguigu.lihaibo.spark.core.framework.util.EnvUtil
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Author: tanggaomeng
  * Date: 2021/1/16 14:42
  * Describe:
  */
trait TApplication {

    def start(master: String = "local[*]", app: String = "Application")(op: => Unit): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster(master).setAppName(app)
        val sc = new SparkContext(sparkConf)

        EnvUtil.put(sc)

        try {
            op
        } catch {
            case ex: Throwable => println(ex.getMessage)
        }

        // TODO 关闭连接
        sc.stop()

        EnvUtil.clear()

    }

}
