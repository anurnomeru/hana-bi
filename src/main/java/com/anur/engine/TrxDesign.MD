### TrxFreeQueuedSynchronizer

TrxFreeQueuedSynchronizer 是lsm数的逻辑入口，包含并发控制，仅写写互斥。TrxFreeQueuedSynchronizer 仅需要流过 “写” 的操作，只是读操作的话，没有必要经过該控制器。

 - acquire方法：在执行之前检查是否需要阻塞，如果需要阻塞则等待唤醒，否则执行业务逻辑。
 - release方法：在执行之前检查是否没有正在阻塞的 acquire 任务，如果有则进行阻塞。如果没有则触发 commit 逻辑。
 
此控制器仅做阻塞控制，没有其他逻辑。

### MemoryMVCCStorageUnCommittedPart

与上面相对应的，是 MemoryMVCCStorageUnCommittedPart，它是数据真正存储的地方，也就是说我们在TrxFreeQueuedSynchronizer中要做的，就是操作这个 UCP 控制器。

 - commonOperate：对应 acquire
 - flushToCommittedPart：对应 release
 
### TrxManager
 
 TrxManager 与上面两者不同，它需要流过所有的操作，主要的作用是拉取当前最低的水位（WaterMark），最低水位便于将数据提交到 lsm 树。
 
### !!! 数据流转

所以数据的流向应该是 经由 
TrxManager ->
(Filter : 写部分) -> TrxFreeQueuedSynchronizer
-> MemoryMVCCStorageUnCommittedPart

提交后：

(Filter : 写部分) -> TrxFreeQueuedSynchronizer
-> MemoryMVCCStorageUnCommittedPart 将数据冲刷到 -> MemoryMVCCStorageCommittedPart
-> TrxManager 刷新水位
-> 通知水位变更到 MemoryMVCCStorageCommittedPart，将数据刷入 MemoryLSM

那么其实，我们查询的数据流向应该为：

MemoryMVCCStorageUnCommittedPart(必须是本事务) -> MemoryMVCCStorageCommittedPart -> MemoryLSM -> LSM