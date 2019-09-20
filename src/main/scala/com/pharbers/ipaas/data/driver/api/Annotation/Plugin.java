package com.pharbers.ipaas.data.driver.api.Annotation;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

/**
 * 功能描述
 *
 * @author dcs
 * @version 0.0
 * @tparam T 构造泛型参数
 * @note 一些值得注意的地方
 * @since 2019/09/16 19:43
 */
@Retention(RetentionPolicy.RUNTIME)
public @interface Plugin {
    String[] args() default {};
    String name() default "";
    String msg() default "";
}
