package com.jason.core;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class ConsistentHash<S> {
    protected int NODE_NUM = 160; // 每个机器节点关联的虚拟节点个数

    protected final TreeMap<Long, S> nodes = new TreeMap<>(); // 虚拟节点

    public void addNode(S node, int index) {
        String preKey = "SHARD-" + index + "-NODE-";
        for (int n = 0; n < NODE_NUM; n++) {
            long hashCode = hash(preKey + n);
            nodes.put(hashCode, node);
        }
    }

    /**
     * 顺时针获取一个最近的节点
     * @param key
     * @return
     */
    public S getClosestNode(String key) {
        long hashCode = hash(key);
        //tailMap(K fromKey) 方法用于返回此映射，其键大于或等于fromKey的部分视图。返回的映射受此映射支持，因此改变返回映射反映在此映射中，反之亦然。
        SortedMap<Long, S> tail = nodes.tailMap(hashCode);
        Map<Long, S> map = tail.size() > 0 ? tail : nodes;
        for (Long vkey : map.keySet()) {
            return map.get(vkey);
        }
        return null;
    }

    /**
     * MurMurHash算法，是非加密HASH算法，性能很高，
     * 比传统的CRC32,MD5，SHA-1（这两个算法都是加密HASH算法，复杂度本身就很高，带来的性能上的损害也不可避免）
     * 等HASH算法要快很多，而且据说这个算法的碰撞率很低. http://murmurhash.googlepages.com/
     */
    private static long hash(String key) {
        ByteBuffer buf = ByteBuffer.wrap(key.getBytes());
        int seed = 0x1234ABCD;

        ByteOrder byteOrder = buf.order();
        buf.order(ByteOrder.LITTLE_ENDIAN);

        long m = 0xc6a4a7935bd1e995L;
        int r = 47;

        long h = seed ^ (buf.remaining() * m);

        long k;
        while (buf.remaining() >= 8) {
            k = buf.getLong();

            k *= m;
            k ^= k >>> r;
            k *= m;

            h ^= k;
            h *= m;
        }

        if (buf.remaining() > 0) {
            ByteBuffer finish = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
            finish.put(buf).rewind();
            h ^= finish.getLong();
            h *= m;
        }

        h ^= h >>> r;
        h *= m;
        h ^= h >>> r;

        buf.order(byteOrder);
        return h;
    }

}
