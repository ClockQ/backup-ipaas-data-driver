package com.pharbers.ipaas.data.driver.config

import java.io.{File, FileInputStream}

import com.pharbers.ipaas.data.driver.config.yamlModel.{Job, JobBean}

/**
  * @author dcs
  * @param $args
  * @tparam T
  * @note
  */
object Config{
    val configReaderMap: Map[String, ConfigReaderTrait] = Map(
        "json" -> JsonReader(),
        "yaml" -> YamlReader()
    )

    def readJobConfig(path: String): Seq[Job] ={
        configReaderMap.getOrElse(path.split('.').last, throw new Exception("不能解析的文件类型")).readObjects[Job](new FileInputStream(new File(path)))
    }
}
