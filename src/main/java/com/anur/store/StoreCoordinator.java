package com.anur.store;

import com.anur.core.coordinate.model.HashGet;
import com.anur.store.core.StoreOperator;

/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 *
 * 外部操作 HASH 命令用，最外层的入口
 */
public class StoreCoordinator {

    private static volatile StoreCoordinator INSTANCE;

    public static StoreCoordinator getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (StoreCoordinator.class) {
                if (INSTANCE == null) {
                    INSTANCE = new StoreCoordinator();
                }
            }
        }

        return INSTANCE;
    }

    /**
     * hash 的 Get 操作
     */
    public String get(HashGet hashGet) {
        return StoreOperator.getINSTANCE()
                            .get(hashGet.key);
    }

    //
    //    /**
    //     * hash 的 Set 操作
    //     */
    //    public String setNx(HashSetNx hashSetNx) {
    //
    //        HashOperator.getINSTANCE()
    //                    .setNx();
    //        return null;
    //    }
}
