package scala.anur.engine.storage

import java.lang

import com.anur.io.engine.storage.core.{HanabiEntry, StorageEngine}

/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
class LsmTreeEngine extends StorageEngine {

    override def strSelect(key: String, trxId: lang.Long): HanabiEntry = ???

    override def strInsert(key: String, entry: HanabiEntry): Unit = ???

    override def strUpdate(key: String, entry: HanabiEntry): Unit = ???

    override def strUpsert(key: String, entry: HanabiEntry): Unit = ???

    override def strDelete(key: String, trxId: lang.Long): Unit = ???

    override def commonStartTrx(): Long = ???

    override def commonCommitTrx(trxId: Long): Unit = ???
}
