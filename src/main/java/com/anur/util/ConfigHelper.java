package com.anur.util;

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Optional;
import java.util.ResourceBundle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import com.anur.exception.HanabiException;
import javafx.util.Pair;

/**
 * Created by Anur IjuoKaruKas on 2019/1/19
 */
public class ConfigHelper {

    protected volatile static ResourceBundle RESOURCE_BUNDLE;

    private static Lock READ_LOCK;

    private static Lock WRITE_LOCK;

    private static String ERROR_FORMATTER = "读取application.properties配置异常，异常项目：%s，建议：%s";

    private static ConcurrentHashMap<ConfigEnum, Object> CACHE = new ConcurrentHashMap<>();

    static {
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        READ_LOCK = readWriteLock.readLock();
        WRITE_LOCK = readWriteLock.writeLock();
        RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
    }

    /**
     * 优先获取缓存中的值，如果获取不到再从配置文件获取
     */
    private static <T> T cacheSupplier(ConfigEnum configEnum, Supplier<T> supplier) {
        T t;
        try {
            READ_LOCK.lock();
            t = (T) CACHE.get(configEnum);
            if (t == null) {
                t = (T) CACHE.get(configEnum);
                if (t == null) {
                    t = supplier.get();
                    CACHE.put(configEnum, t);
                }
            }
        } finally {
            READ_LOCK.unlock();
        }
        return t;
    }

    /**
     * 刷新配置
     */
    public static void refresh() {
        try {
            WRITE_LOCK.lock();
            CACHE.clear();
            RESOURCE_BUNDLE = ResourceBundle.getBundle("application");
        } finally {
            WRITE_LOCK.unlock();
        }
    }

    /**
     * 根据key获取某个配置
     */
    protected static <T> T getConfig(ConfigEnum configEnum, Function<String, T> transfer) {
        return cacheSupplier(configEnum, () -> Optional.of(RESOURCE_BUNDLE.getString(configEnum.getKey()))
                                                       .map(transfer)
                                                       .orElseThrow(() -> new ApplicationConfigException(String.format(ERROR_FORMATTER, configEnum.getKey(), configEnum.getAdv()))));
    }

    /**
     * 根据key模糊得获取某些配置，匹配规则为 key%
     */
    protected static <T> List<T> getConfigSimilar(ConfigEnum configEnum, Function<Pair<String, String>, T> transfer) {
        return cacheSupplier(configEnum, () -> {
            List<T> tList;
            try {
                Enumeration<String> stringEnumeration = RESOURCE_BUNDLE.getKeys();
                List<String> keys = new ArrayList<>();
                while (stringEnumeration.hasMoreElements()) {
                    String k = stringEnumeration.nextElement();
                    if (k.startsWith(configEnum.getKey())) {
                        keys.add(k);
                    }
                }
                String key = configEnum.key;
                tList = keys.stream()
                            .map(k -> {
                                String subKey = k.length() > key.length() ? k.substring(key.length() + 1) : k;
                                String val = RESOURCE_BUNDLE.getString(k);
                                return transfer.apply(new Pair<>(subKey, val));
                            })
                            .collect(Collectors.toList());
            } catch (Throwable e) {
                throw new ApplicationConfigException(String.format(ERROR_FORMATTER, configEnum.getKey(), configEnum.getAdv()));
            }
            return tList;
        });
    }

    public static class ApplicationConfigException extends HanabiException {

        public ApplicationConfigException(String message) {
            super(message);
        }
    }

    public enum ConfigEnum {
        SERVER_PORT("server.port", "server.port 是本机的对外端口号配置，请检查配置是否正确。"),

        SERVER_NAME("server.name", "server.name 是本机的服务名，应唯一。"),

        CLIENT_ADDR("client.addr", "client.addr 的配置格式应由如下组成：client.addr.{服务名}:{选举leader使用端口号}:{集群内机器通讯使用端口号}");

        ConfigEnum(String key, String adv) {
            this.key = key;
            this.adv = adv;
        }

        public String getKey() {
            return key;
        }

        public String getAdv() {
            return adv;
        }

        private String key;

        private String adv;
    }
}