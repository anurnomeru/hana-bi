package com.anur.core.coordinate.apis

import com.anur.core.elect.ElectMeta
import com.anur.core.elect.model.GenerationAndOffset
import com.anur.core.lock.ReentrantReadWriteLocker
import com.anur.io.store.OffsetManager
import org.slf4j.LoggerFactory
import java.util.HashMap
import java.util.HashSet
import java.util.concurrent.ConcurrentSkipListMap

/**
 * Created by Anur IjuoKaruKas on 2019/7/9
 *
 * 日志一致性控制器 Leader 端
 */
object LeaderCoordinateManager : ReentrantReadWriteLocker() {

    private val logger = LoggerFactory.getLogger(LeaderCoordinateManager::class.java)

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
            .load()

        if (!ElectMeta.isLeader) {
            logger.error("不是leader不太可能收到 fetchReport！ 很可能是有BUG ")
            return latestGAO
        }

        if (latestGAO > GAO) {// 小于已经 commit 的 GAO 无需记录
            return latestGAO
        }

        val currentGAO = readLockSupplier<GenerationAndOffset> { currentFetchMap[node] }

        return if (currentGAO != null && currentGAO >= GAO) {// 小于之前提交记录的无需记录
            latestGAO
        } else writeLockSupplier<GenerationAndOffset> {
            if (currentGAO != null) {// 移除之前的 commit 记录
                logger.debug("节点 {} fetch 进度由 {} 更新到了进度 {}", node, currentGAO.toString(), GAO.toString())
                fetchMap[currentGAO]!!.remove(node)
            } else {
                logger.debug("节点 {} 已经 fetch 更新到了进度 {}", node, GAO.toString())
            }

            currentFetchMap[node] = GAO// 更新新的 commit 记录
            fetchMap.compute(GAO) { // 更新新的 commit 记录
                _, strings ->
                var result = strings
                if (result == null) {
                    result = mutableSetOf()
                }
                result.add(node)
                result
            }

            var approach: GenerationAndOffset? = null
            for (entry in fetchMap.entries) {
                if (entry.value
                        .size + 1 >= ElectMeta.quorom) {// +1 为自己的一票
                    approach = entry.key
                }
            }

            if (approach == null) {
                return@writeLockSupplier latestGAO
            } else {
                logger.info("进度 {} 已可提交 ~ 已经拟定 approach，半数节点同意则进行 commit", GAO.toString())
                return@writeLockSupplier approach
            }
        }

    }

    fun commitReport(node: String, commitGAO: GenerationAndOffset) {
        val latestCommitGAO = OffsetManager.getINSTANCE()
            .load()

        if (latestCommitGAO.compareTo(commitGAO) > 0) {// 小于已经 commit 的 GAO 直接无视
            return
        }

        val currentCommitGAO = readLockSupplier<GenerationAndOffset> { currentCommitMap[node] }
        if (currentCommitGAO != null && currentCommitGAO >= commitGAO) {// 小于之前提交记录的无需记录
            return
        }

        writeLockSupplier<GenerationAndOffset> {
            if (currentCommitGAO != null) {// 移除之前的 commit 记录
                logger.debug("节点 {} 已经 commit 进度由 {} 更新到了进度 {}", node, currentCommitGAO.toString(), commitGAO.toString())
                commitMap[currentCommitGAO]!!.remove(node)
            } else {
                logger.debug("节点 {} 已经 fetch 更新到了进度 {}", node, commitGAO.toString())
            }

            currentCommitMap[node] = commitGAO// 更新新的 commit 记录
            commitMap.compute(commitGAO) { // 更新新的 commit 记录
                _, strings ->
                var result = strings
                if (result == null) {
                    result = HashSet()
                }
                result.add(node)
                result
            }

            var approach: GenerationAndOffset? = null
            for (entry in commitMap.entries) {
                if (entry.value
                        .size + 1 >= ElectMeta.quorom) {// +1 为自己的一票
                    approach = entry.key
                }
            }

            if (approach == null) {
                return@writeLockSupplier latestCommitGAO
            } else {
                logger.info("进度 {} 已经完成 commit ~", commitGAO.toString())
                OffsetManager.getINSTANCE()
                    .cover(commitGAO)
                return@writeLockSupplier approach
            }
        }
    }
}