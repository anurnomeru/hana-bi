package com.anur.util;

/**
 * Created by Anur IjuoKaruKas on 2019/11/12
 */
public class BitUtil {

    public static String get32BitBinString(int num) {
        StringBuilder sBuilder = new StringBuilder();
        for (int i = 0; i < 32; i++) {
            sBuilder.append(num & 1);
            num = num >>> 1;
        }
        return sBuilder.reverse()
                       .toString();
    }
    
    public static String get64BitBinString(long num) {
        StringBuilder sBuilder = new StringBuilder();
        for (int i = 0; i < 64; i++) {
            sBuilder.append(num & 1);
            num = num >>> 1;
        }
        return sBuilder.reverse()
                   .toString();
    }
}
