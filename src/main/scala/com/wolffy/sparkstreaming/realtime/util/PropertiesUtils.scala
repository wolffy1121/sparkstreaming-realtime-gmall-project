package com.wolffy.sparkstreaming.realtime.util

import java.util.ResourceBundle

/**
 * 配置解析类
 */
object PropertiesUtils {

    //直接读 properties 文件，不需要后缀
    private val bundle: ResourceBundle = ResourceBundle.getBundle("config")

    def apply(key : String ):String ={
        bundle.getString(key)
    }

    def main(args: Array[String]): Unit = {
        println(PropertiesUtils("kafka.bootstrap.servers"))
    }
}
