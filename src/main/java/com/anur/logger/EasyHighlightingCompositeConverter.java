package com.anur.logger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.color.ANSIConstants;
import ch.qos.logback.core.pattern.color.ForegroundCompositeConverterBase;

/**
 * 作者：ZJK-order
 * 来源：CSDN
 * 原文：https://blog.csdn.net/u012693119/article/details/79716306
 * 版权声明：本文为博主原创文章，转载请附上博文链接！
 *
 * Created by Anur IjuoKaruKas on 1/25/2019
 */
public class EasyHighlightingCompositeConverter extends ForegroundCompositeConverterBase<ILoggingEvent> {

    /**
     * (non-Javadoc)
     *
     * @see ch.qos.logback.core.pattern.color.ForegroundCompositeConverterBase#
     * getForegroundColorCode(java.lang.Object)
     */
    protected String getForegroundColorCode(ILoggingEvent event) {
        switch (event.getLevel()
                     .toInt()) {
        case Level.ERROR_INT:
            return ANSIConstants.RED_FG;
        case Level.WARN_INT:
            return ANSIConstants.YELLOW_FG;
        case Level.INFO_INT:
            return ANSIConstants.GREEN_FG;
        case Level.DEBUG_INT:
            return ANSIConstants.MAGENTA_FG;
        default:
            return null;
        }
    }
}