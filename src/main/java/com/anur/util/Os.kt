package com.anur.util

import java.util.Locale


/**
 * Created by Anur IjuoKaruKas on 2019/9/23
 */
object Os {
    val name = System.getProperty("os.name").toLowerCase(Locale.ROOT)
    val isWindows = name.startsWith("windows")
}