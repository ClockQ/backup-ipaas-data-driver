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

package com.pharbers.ipaas.data.driver.api.factory

import com.pharbers.ipaas.data.driver.api.work._

/** iPaas Driver 运行实体工厂
 *
 * @author dcs
 * @version 0.1
 * @since 2019/06/14 15:26
 */
trait PhFactoryTrait[T <: PhWorkTrait[_]] {
    /** 构建运行实例
     *
     * @return _root_.com.pharbers.ipaas.data.driver.api.work.PhWorkTrait[T]
     * @author dcs
     * @version 0.1
     * @since 2019/6/11 16:30
     */
    def inst(): T
}
