package com.jason;

import com.jason.core.RedisRing;
import com.jason.core.RedisShard;
import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class Test {

    @org.junit.Test
    public void loadRedisShard(){
        List<RedisShard> list = new ArrayList<RedisShard>();
        RedisShard redisShard1 = new RedisShard("192.168.1.91:3308","1 123 2");
        RedisShard redisShard2 = new RedisShard("192.168.1.92:3308","1 123 2");
        RedisShard redisShard3 = new RedisShard("192.168.1.93:3308","1 123 2");
        RedisShard redisShard4 = new RedisShard("192.168.1.94:3308","1 123 2");
        RedisShard redisShard5 = new RedisShard("192.168.1.95:3308","1 123 2");
        list.add(redisShard1);
        list.add(redisShard2);
        list.add(redisShard3);
        list.add(redisShard4);
        list.add(redisShard5);
        RedisRing redisRing = new RedisRing(list);
    }

    @org.junit.Test
    public void treeMapTest(){
        TreeMap<Integer, String> treemap = new TreeMap<Integer, String>();
        SortedMap<Integer, String> treemapincl = new TreeMap<Integer, String>();

        // populating tree map
        treemap.put(2, "two");
        treemap.put(1, "one");
        treemap.put(3, "three");
        treemap.put(6, "six");
        treemap.put(5, "five");

        System.out.println("Getting tail map");
        treemapincl=treemap.tailMap(3);
        System.out.println("Tail map values: "+treemapincl);
    }
}
