package cn.edu360.shangguigu.lihaibo.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * Author: tanggaomeng
  * Date: 2021/1/20 17:30
  * Describe:
  */
object SparkSQL10_Hive {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
        val spark: SparkSession = SparkSession
          .builder()
          .enableHiveSupport()  // 必须启动Hive支持
          .config(sparkConf)
          .getOrCreate()
        import spark.implicits._

        // TODO 使用SparkSQL连接外置的Hive
        //  1.拷贝hive-site.xml文件到classpath下
        //  2.启动hive的支持
        //  3.增加对应的依赖关系（包含MySQL驱动）

        spark.sql("show tables").show()


        spark.close()

    }

}
