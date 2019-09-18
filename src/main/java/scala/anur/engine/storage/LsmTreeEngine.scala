package scala.anur.engine.storage

import java.lang

import com.anur.io.engine.storage.StorageEngine
import com.anur.io.engine.storage.core.HanabiEntry

/**
 * Created by Anur IjuoKaruKas on 2019/9/18
 */
class LsmTreeEngine extends StorageEngine {

    override def select(key: String, trxId: lang.Long): HanabiEntry = ???

    override def insert(key: String, entry: HanabiEntry): Unit = ???

    override def update(key: String, entry: HanabiEntry): Unit = ???

    override def upsert(key: String, entry: HanabiEntry): Unit = ???

    override def delete(key: String, trxId: lang.Long): Unit = ???

    override def startTrx(): Long = ???

    override def commit(trxId: Long): Unit = ???
}
