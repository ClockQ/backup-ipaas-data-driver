package com.pharbers.ipaas.data.driver.config

import java.io.{File, FileInputStream}

import com.pharbers.ipaas.data.driver.config.yamlModel.{Job, JobBean}

/** 功能描述
  *读取配置文件
  * @author dcs
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note 一些值得注意的地方
  */
object Config{
    val configReaderMap: Map[String, ConfigReaderTrait] = Map(
        "json" -> JsonReader(),
        "yaml" -> YamlReader()
    )
/** 功能描述
  *
从job配置文件生成配置实例
  * @param path 配置文件路径
  * @return scala.Seq[_root_.com.pharbers.ipaas.data.driver.config.yamlModel.Job]
  * @author EDZ
  * @version 0.0
  * @since 2019/6/11 15:27
  * @note 只能读取json和yaml
  * @example {{{这是一个例子}}}
  */
    def readJobConfig(path: String): Seq[Job] ={
        configReaderMap.getOrElse(path.split('.').last, throw new Exception("不能解析的文件类型")).readObjects[Job](new FileInputStream(new File(path)))
    }
}
