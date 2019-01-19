package com.anur.logger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.pattern.color.ANSIConstants;
import ch.qos.logback.core.pattern.color.ForegroundCompositeConverterBase;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
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
