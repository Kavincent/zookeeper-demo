package com.ygm.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;

/**
 * Created by admin on 2019/1/26.
 */
public class CuratorDemo {
    public static void main(String[] args) throws Exception {
        CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder().connectString("127.0.0.1").sessionTimeoutMs(4000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .namespace("curator").build();

        curatorFramework.start();

        // 结果：/curator/ygm/node1
        // 原生api中，必须市逐层创建，也就是父节点必须存在,字节点才能创建
        curatorFramework.create().creatingParentsIfNeeded()
                .withMode(CreateMode.PERSISTENT)
                .forPath("/ygm/node1","1".getBytes());


        Stat stat = new Stat();
        curatorFramework.getData().storingStatIn(stat).forPath("/ygm/node1");

        curatorFramework.setData().withVersion(stat.getVersion()).forPath("/ygm/node1","xx".getBytes());

        // 删除
      //  curatorFramework.delete().deletingChildrenIfNeeded().forPath("/ygm/node1");

        curatorFramework.close();



    }
}
