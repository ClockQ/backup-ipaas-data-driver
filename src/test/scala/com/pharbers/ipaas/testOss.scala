///*
// * This file is part of com.pharbers.ipaas-data-driver.
// *
// * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
// * it under the terms of the GNU General Public License as published by
// * the Free Software Foundation, either version 3 of the License, or
// * (at your option) any later version.
// *
// * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
// */
//
//package com.pharbers.ipaas
//
//import java.io.{BufferedReader, File, InputStreamReader, PrintWriter}
//import java.util.UUID
//
//import com.aliyun.oss.{OSS, OSSClientBuilder}
//import com.aliyun.oss.model.OSSObject
//import com.pharbers.ipaas.data.driver.api.model.Job
//import com.pharbers.ipaas.data.driver.libs.input.JsonInput
//import net.iharder.Base64.InputStream
//import org.scalatest.FunSuite
//
///** 功能描述
//  *
//  * @param args 构造参数
//  * @tparam T 构造泛型参数
//  * @author dcs
//  * @version 0.0
//  * @since 2019/08/01 16:35
//  * @note 一些值得注意的地方
//  */
//class testOss extends FunSuite {
//    test("oss") {
//        val endpoint = "oss-cn-beijing.aliyuncs.com"
//        val accessKeyId = "LTAIEoXgk4DOHDGi"
//        val accessKeySecret = "x75sK6191dPGiu9wBMtKE6YcBBh8EI"
//        val client: OSS = new OSSClientBuilder().build(endpoint, accessKeyId, accessKeySecret)
//        val ossObj: OSSObject = client.getObject("pharbers-resources", "pressureTest.json")
////        val file = new File("oss.json")
////        file.deleteOnExit()
////        file.createNewFile()
////        val writer = new PrintWriter(file)
////        val reader = new BufferedReader(new InputStreamReader(ossObj.getObjectContent))
////        var a = 0
////        while (a < 118){
////            if(reader.ready()) {
////                val s = reader.readLine()
////                println(s)
////                writer.write(s)
////                a = a + 1
////            }
////        }
////        writer.close()
//        val stream = ossObj.getObjectContent
//        val job = JsonInput().readObjects[Job](stream)
//    }
//}
