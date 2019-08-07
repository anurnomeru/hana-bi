package com.anur.config;

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
import com.anur.exception.ApplicationConfigException;
import javafx.util.Pair;
import jdk.nashorn.internal.ir.annotations.Ignore;

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
    private static <T> T lockSupplier(ConfigEnum configEnum, Supplier<T> supplier) {
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
        return lockSupplier(configEnum, () -> Optional.of(RESOURCE_BUNDLE)
                                                      .map(resourceBundle -> {
                                                          String config = null;
                                                          try {
                                                              config = resourceBundle.getString(configEnum.getKey());
                                                          } catch (Exception ignore) {
                                                          }
                                                          return config;
                                                      })
                                                      .map(transfer)
                                                      .orElseThrow(() -> new ApplicationConfigException(String.format(ERROR_FORMATTER, configEnum.getKey(), configEnum.getAdv()))));
    }

    /**
     * 根据key模糊得获取某些配置，匹配规则为 key%
     */
    protected static <T> List<T> getConfigSimilar(ConfigEnum configEnum, Function<Pair<String, String>, T> transfer) {
        return lockSupplier(configEnum, () -> {
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

    protected enum ConfigEnum {

        ////////////////////// InetSocketAddressConfigHelper

        SERVER_NAME("server.name", "server.name 是本机的服务名，集群内应唯一"),

        CLIENT_ADDR("client.addr", "client.addr 的配置格式应由如下组成：client.addr.{服务名}:{选举leader使用端口号}:{集群内机器通讯使用端口号}"),

        ////////////////////// LogConfigHelper

        LOG_BASE_PATH("log.base.path", "log日志的基础目录，默认在项目下"),

        LOG_INDEX_INTERVAL("log.indexInterval", "log.IndexInterval 是操作日志索引生成时的字节间隔，有助于节省空间，不设定的太小都可"),

        LOG_MAX_INDEX_SIZE("log.maxIndexSize", "log.maxIndexSize 日志文件最大只能为这么大，太大了影响索引效率，日志分片受其影响，如果索引文件满了，那么将会创建新的日志分片。"),

        LOG_MAX_MESSAGE_SIZE("log.maxMessageSize", "log.maxMessageSize 是操作日志最大的大小，它也影响我们的操作 key + value 最大的大小"),

        LOG_MAX_SEGMENT_SIZE("log.maxSegmentSize", "log.maxSegmentSize 日志分片文件大小"),

        ////////////////////// ElectConfigHelper

        ELECT_ELECTION_TIMEOUT_MS("elect.electionTimeoutMs", "超过 ms 没收取到 leader 的心跳就会发起选举"),

        ELECT_VOTES_BACK_OFF_MS("elect.votesBackOffMs", "选举期间拉票频率，建议为 electionTimeoutMs 的一半"),

        ELECT_HEART_BEAT_MS("elect.heartBeatMs", "心跳包，建议为 electionTimeoutMs 的一半"),

        ////////////////////// CoordinateConfigHelper

        COORDINATE_FETCH_BACK_OFF_MS("coordinate.fetchBackOffMs", "间隔 ms 去 leader 拉取最新的日志");

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