package env

import java.io._
import com.pharbers.ipaas.data.driver.libs.input._
import com.pharbers.ipaas.data.driver.api.factory._
import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.api.work.PhJobTrait

/** Config 工具实例
  *
  * @author clock
  * @version 0.1
  * @since 2019/06/14 14:42
  * @note 测试使用，随时删除
  */
@deprecated
object configObj {
    val configReaderMap: Map[String, InputTrait] = Map(
        "json" -> JsonInput(),
        "yaml" -> YamlInput()
    )

    /** 从job配置文件生成配置实例
      *
      * @param path 配置文件路径
      * @return scala.Seq[_root_.com.pharbers.ipaas.data.driver.config.yamlModel.Job]
      * @author doc
      * @version 0.1
      * @since 2019/6/11 15:27
      * @note 只能读取json和yaml
      * @example {{{这是一个例子}}}
      */
    def readJobConfig(path: String): Seq[Job] = {
        configReaderMap.getOrElse(path.split('.').last, throw new Exception("不能解析的文件类型")).readObjects[Job](new FileInputStream(new File(path)))
    }

    def inst(jobs: Seq[Job]): Seq[PhJobTrait] = {
        jobs.map(x => getMethodMirror(x.getFactory)(x).asInstanceOf[PhFactoryTrait[PhJobTrait]].inst())
    }
}
