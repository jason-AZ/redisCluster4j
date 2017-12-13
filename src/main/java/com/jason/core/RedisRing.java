package com.jason.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class RedisRing {
    private final ConsistentHash<RedisShard> hashRing = new ConsistentHash<>();

    private final Map<Integer, RedisShard> shards = new TreeMap<>();

    private final int minIndex;

    private final int maxIndex;

    public RedisRing(Collection<RedisShard> shards) {
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;
        for (RedisShard shard : shards) {
            this.hashRing.addNode(shard, shard.getDefaultDb());
            this.shards.put(shard.getIndex(), shard);
            min = Math.min(min, shard.getIndex());
            max = Math.max(max, shard.getIndex());
        }
        this.minIndex = min;
        this.maxIndex = max;
    }

    public Collection<RedisShard> getShards() {
        return shards.values();
    }

    public List<RedisShard> getAvailableShards() {
        List<RedisShard> availableShards = new ArrayList<>(shards.size());
        shards.values().forEach(shard ->
        {
            if (shard.isAvailable()) {
                availableShards.add(shard);
            }
        });
        return availableShards;
    }

    public RedisShard getMainShard(String key) {
        return this.hashRing.getClosestNode(key);
    }

    public RedisShard getMainShard(RedisShard backupShard) {
        int backUpIndex = backupShard.getIndex();
        int mainIndex = backUpIndex > this.minIndex ? backUpIndex - 1 : this.maxIndex;
        return this.shards.get(mainIndex);
    }

    public RedisShard getBackUpShard(RedisShard mainShard) {
        int mainIndex = mainShard.getIndex();
        int backUpIndex = mainIndex < this.maxIndex ? mainIndex + 1 : this.minIndex;
        return this.shards.get(backUpIndex);
    }

    public String getKeyLocation(String key) {
        return getMainShard(key).getAddress();
    }

    public void rebuildPool() {
        shards.values().forEach(shard -> shard.loadPool());
    }

    public void destroy() {
        shards.values().forEach(shard ->
        {
            shard.closePool();
        });
        this.shards.clear();
        hashRing.nodes.clear();
    }

}