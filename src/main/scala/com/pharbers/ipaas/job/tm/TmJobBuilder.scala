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

package com.pharbers.ipaas.job.tm

import com.pharbers.ipaas.data.driver.api.model.Job
import com.pharbers.ipaas.data.driver.libs.input.JsonInput
import com.sun.xml.internal.messaging.saaj.util.ByteInputStream

/** 功能描述
  *
  * @param jobMode 模板
  * @author dcs
  * @version 0.0
  * @since 2019/08/13 14:07
  * @note 铁马job builder， 用于动态填入jobId和 projectId  periodId proposalId
  */
case class TmJobBuilder(jobMode: Job, jobId: String, jobType: String) {
    jobMode.jobId = jobId
    jobMode.jobType = jobType
    private var jobJson = JsonInput.mapper.writeValueAsString(jobMode)

    def setMongoSourceFilter(config: Map[String, String]): TmJobBuilder ={
        jobJson = jobJson.replaceAll("#proposalId#", config.getOrElse("proposalId", ""))
                .replaceAll("#projectId#", config.getOrElse("projectId", ""))
                .replaceAll("#periodId#", config.getOrElse("periodId", ""))
                .replaceAll("#phase#", config.getOrElse("phase", "0"))
        this
    }

    def build(): Job ={
        JsonInput().readObject[Job](jobJson)
    }
}
