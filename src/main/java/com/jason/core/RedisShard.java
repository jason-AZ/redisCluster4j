package com.jason.core;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisShard {
    public static final JedisPoolConfig poolConfig = new JedisPoolConfig();

    public static int soTimeout = 5000;

    public static int coTimeout = 1000;

    private final String ip;

    private final int port;

    private final int defaultDb;

    private final int index;

    private final String password;

    private JedisPool pool;

    private boolean available = false;

    private boolean recovering = false;

    public RedisShard(String address, String property) {
        String[] addr = address.split(":");
        this.ip = addr[0];
        this.port = Integer.valueOf(addr[1]);
        String[] properties = property.split("\\s+");
        this.defaultDb = Integer.parseInt(properties[0]);
        this.password = "null".equalsIgnoreCase(properties[1]) ? null : properties[1];
        this.index = Integer.parseInt(properties[2]);
        loadPool();
    }

    public void loadPool() {
        closePool();
        JedisPool pool = new JedisPool(poolConfig, ip, port, coTimeout, soTimeout, password, defaultDb, null,false,null,null,null);
//        LogUtils.logger().info("##### 创建连接池 ip:{}, port:{}, coTimeout:{}, soTimeout:{}, db:{}", ip, port, coTimeout, soTimeout, defaultDb);
        this.pool = pool;
    }

    public String getAddress() {
        return this.ip + ":" + this.port;
    }

    public boolean isAvailable() {
        return available;
    }

    public void setAvailable(boolean available) {
        this.available = available;
    }

    public boolean isRecovering() {
        return recovering;
    }

    public void setRecovering(boolean recovering) {
        this.recovering = recovering;
    }

    public Jedis getJedis() {
        return this.pool.getResource();
    }

    public void closePool() {
        if (this.pool != null) {
            this.pool.close();
            this.pool = null;
        }
    }

    public int getDefaultDb() {
        return defaultDb;
    }

    public int getIndex() {
        return index;
    }

    @Override
    public String toString() {
        return "[" + getAddress() + "]";
    }

}
