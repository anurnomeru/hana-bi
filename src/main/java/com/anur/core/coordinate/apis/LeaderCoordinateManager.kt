package com.anur.core.coordinate.apis

import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.lock.ReentrantReadWriteLocker
import com.anur.io.store.OffsetManager
import org.slf4j.LoggerFactory
import java.util.HashMap
import java.util.HashSet
import java.util.concurrent.ConcurrentSkipListMap
import java.util.function.Supplier

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 *
 * 日志一致性控制器 Leader 端
 */
object LeaderCoordinateManager : ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(LeaderCoordinateManager.javaClass)

    /**
     * 作为 Leader 时有效，维护了每个节点的 fetch 进度
     */
    @Volatile
    private var fetchMap = ConcurrentSkipListMap<GenerationAndOffset, MutableSet<String>>()

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 fetch
     */
    @Volatile
    private var currentFetchMap = HashMap<String, GenerationAndOffset>()

    /**
     * 作为 Leader 时有效，维护了每个节点的 commit 进度
     */
    @Volatile
    private var commitMap = ConcurrentSkipListMap<GenerationAndOffset, MutableSet<String>>()

    /**
     * 作为 Leader 时有效，记录了每个节点最近的一次 commit
     */
    @Volatile
    private var currentCommitMap = HashMap<String, GenerationAndOffset>()

    /**
     * Follower 向 Leader 提交拉取到的最大的 GAO
     *
     * 如果某个最大的 GAO 已经达到了 commit 条件，将返回此 GAO。
     */
    fun fetchReport(node: String, GAO: GenerationAndOffset): GenerationAndOffset {
        val latestGAO = OffsetManager.getINSTANCE()
            .load()!!

        if (!ElectMeta.isLeader) {
            logger.error("不是leader不太可能收到 fetchReport！ 很可能是有BUG ")
            return latestGAO
        }

        if (latestGAO > GAO) {// 小于已经 commit 的 GAO 无需记录
            return latestGAO
        }

        val currentGAO = readLockSupplier(Supplier { currentFetchMap[node] })

        return if (currentGAO != null && currentGAO >= GAO) {// 小于之前提交记录的无需记录
            latestGAO
        } else {
            writeLockSupplierCompel(Supplier {
                // 移除之前的 fetch 记录
                currentGAO?.also {
                    logger.info("节点 {} fetch 进度由 {} 更新到了进度 {}", node, it.toString(), GAO.toString())
                    fetchMap[it]!!.remove(node)
                } ?: logger.info("节点 {} 已经 fetch 更新到了进度 {}", node, GAO.toString())

                currentFetchMap[node] = GAO// 更新节点的 fetch 进度
                fetchMap.compute(GAO) { // 更新节点最近一次 fetch 处于哪个 GAO
                    _, strings ->
                    (strings ?: mutableSetOf()).also {
                        it.add(node)
                    }
                }

                // 找到最大的那个票数 >= quorum 的 fetch GAO
                fetchMap.entries.findLast { e -> e.value.size >= ElectMeta.quorum }?.key.also { logger.info("进度 {} 已可提交 ~ 已经拟定 approach，半数节点同意则进行 commit", it.toString()) }
                    ?: latestGAO
            })
        }
    }

    fun commitReport(node: String, commitGAO: GenerationAndOffset) {
        val latestCommitGAO = OffsetManager.getINSTANCE()
            .load()!!

        if (!ElectMeta.isLeader) {
            logger.error("不是leader不太可能收到 commitReport！ 很可能是有BUG ")
            return
        }

        if (latestCommitGAO > commitGAO) {// 小于已经 commit 的 GAO 直接无视
            return
        }

        val currentCommitGAO = readLockSupplier(Supplier { currentCommitMap[node] })
        if (currentCommitGAO != null && currentCommitGAO >= commitGAO) {// 小于之前提交记录的无需记录
            return
        }

        writeLockSupplierCompel(Supplier {
            // 移除之前的 commit 记录
            currentCommitGAO?.also {
                logger.info("节点 {} 已经 commit 进度由 {} 更新到了进度 {}", node, it.toString(), commitGAO.toString())
                commitMap[it]!!.remove(node)
            } ?: logger.info("节点 {} 已经 fetch 更新到了进度 {}", node, commitGAO.toString())


            currentCommitMap[node] = commitGAO//更新节点的 commit 进度
            commitMap.compute(commitGAO) { // 更新节点最近一次 commit 处于哪个 GAO
                _, strings ->
                (strings ?: mutableSetOf()).also {
                    it.add(node)
                }
            }

            // 找到最大的那个票数 >= quorum 的 commit GAO
            commitMap.entries.findLast { e -> e.value.size >= ElectMeta.quorum }?.key.also { logger.info("进度 {} 已经完成 commit ~", it.toString()) }
                ?: latestCommitGAO
        })
    }
}