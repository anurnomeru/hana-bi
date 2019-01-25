package com.anur.logger;

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import ch.qos.logback.classic.pattern.ClassicConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Created by Anur IjuoKaruKas on 1/25/2019
 *
 * 作者：ZJK-order
 * 来源：CSDN
 * 原文：https://blog.csdn.net/u012693119/article/details/79716306
 * 版权声明：本文为博主原创文章，转载请附上博文链接！
 */
public class ProcessIdClassicConverter extends ClassicConverter {

    /**
     * (non-Javadoc)
     *
     * @see ch.qos.logback.core.pattern.Converter#convert(java.lang.Object)
     */
    public String convert(ILoggingEvent event) {
        RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
        String name = runtime.getName();
        return name.substring(0, name.indexOf("@"));
    }
}
