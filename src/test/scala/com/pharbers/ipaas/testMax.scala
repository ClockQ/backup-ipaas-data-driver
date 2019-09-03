/*
 * This file is part of com.pharbers.ipaas-data-driver.
 *
 * com.pharbers.ipaas-data-driver is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * com.pharbers.ipaas-data-driver is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Foobar.  If not, see <https://www.gnu.org/licenses/>.
 */

package com.pharbers.ipaas

import java.util.UUID

import com.pharbers.ipaas.data.driver.libs.spark.PhSparkDriver
import com.pharbers.ipaas.data.driver.libs.spark.util.{readParquet, save2Csv}


/** 功能描述
  *
  * @param args 构造参数
  * @tparam T 构造泛型参数
  * @author dcs
  * @version 0.0
  * @since 2019/08/20 17:40
  * @note 一些值得注意的地方
  */
object updown extends App {

    import org.apache.hadoop.conf.Configuration
    import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
    implicit val sparkDriver = PhSparkDriver("cui-test")
    val name = "67143979-bf71-40c0-8ea2-32841f8d084f\n7a81336c-c555-43b4-8665-ab4e77994c53\na331d62c-8365-4ebb-8a07-9c6e3719ea29\n1057fcae-f716-4584-8b71-345c13fb3754\n142b7f5d-cc4a-4957-9ceb-401e2e132de0\n794cbe9c-d1f6-4d68-9c22-7de69b37f86b\n30c7abd1-1166-4b9c-ad5c-e75874fc92a5\n1ccbe8b6-2a39-4dcb-8c97-00c008f7ca99\n414178a8-7635-4fe4-bc91-46d2e57253fc\n8e9ae7f3-4b00-4e71-b5f3-a5c906d691b6\n246b23c1-5147-4500-ae63-b27b6ec08ccc\nc4441148-bebc-442d-a415-82562cdba83a\nd5cf5902-3114-43be-928e-5593b0e58c88\nc5975e84-762a-4db7-aaab-7686d22ea175\n6f7cd770-005b-4106-8a9f-89b8adff380d\n06b76640-4157-40aa-9e23-8a37c1a0331f\n69ba4403-5284-4f2a-9e7f-06956f233aea\n469834b3-281b-4129-b4ed-fae44ce40e43\n11b8166f-9d90-46fa-8669-254445246da9\n8ecb8c3a-cf29-4b7e-9d9f-9af639719617\n3e8a7d5a-4960-4fce-8546-f17470fd3e18\ndc101e95-2e20-4785-9873-cf3c5b3f173d\n5282719f-f9af-4d07-9867-cb28559b14b8"
            .split("\n").map(x => x.trim)

    val path = "/workData/Max"
    val conf = new Configuration
    val hdfs = FileSystem.get(conf)
    val hdfsPath = new Path(path)
    val id = UUID.randomUUID().toString

    val filePathlst = hdfs.listStatus(hdfsPath)
    name.foreach { x  =>
        println(x)
        val df = sparkDriver.setUtil(readParquet()).readParquet(s"$path/$x")
        sparkDriver.setUtil(save2Csv()).save2Csv(df, s"/test/dcs/testMerge/$x")
        hdfs.copyToLocalFile(true, new Path(s"/test/dcs/testMerge/$x"), new Path("D:\\文件\\test"), true)
    }

}

