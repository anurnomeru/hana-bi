package com.anur.store.hash;
import com.anur.core.coordinate.model.HashGet;
import com.anur.store.hash.core.HashOperator;

/**
 * Created by Anur IjuoKaruKas on 2/15/2019
 *
 * 外部操作 HASH 命令用，最外层的入口
 */
public class HashStore {

    private static volatile HashStore INSTANCE;

    public static HashStore getINSTANCE() {
        if (INSTANCE == null) {
            synchronized (HashStore.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HashStore();
                }
            }
        }

        return INSTANCE;
    }

    /**
     * hash 的 Get 操作
     */
    public String get(HashGet hashGet) {
        return HashOperator.getINSTANCE()
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
