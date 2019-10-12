package com.anur.exception


/**
 * Created by Anur IjuoKaruKas on 2019/10/12
 */
class UnSupportTransException : HanabiException("不支持此类型的事务操作！请确认是否版本问题，或者出现了不可预计的 BUG")