package com.anur.exception


/**
 * Created by Anur IjuoKaruKas on 2019/10/12
 */
class UnexpectedException(s: String? = null) : HanabiException(s ?: "出现了不可预计的 BUG") {}