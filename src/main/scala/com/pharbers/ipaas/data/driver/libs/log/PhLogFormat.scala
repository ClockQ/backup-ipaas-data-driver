package com.pharbers.ipaas.data.driver.libs.log

import com.pharbers.ipaas.data.driver.api.work.{PhFuncArgs, PhListArgs, PhStringArgs}



/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/09/09 14:50
  * @note 一些值得注意的地方
  */
case class PhLogFormat(formatMsg: Seq[Any] => String){
    def get(): PhFuncArgs[PhListArgs[PhStringArgs], PhStringArgs] ={
        PhFuncArgs(func)
    }

    private def func(list: PhListArgs[PhStringArgs]): PhStringArgs ={
        PhStringArgs(formatMsg(list.get.map(x => x.get)))
    }
}
