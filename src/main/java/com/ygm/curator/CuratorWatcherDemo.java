package com.ygm.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by admin on 2019/1/26.
 */
public class CuratorWatcherDemo {
    public static void main(String[] args) throws Exception {
       /* CuratorFramework curatorFramework = CuratorFrameworkFactory
                .builder().connectString("127.0.0.1").sessionTimeoutMs(4000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3)).build();
               // .namespace("curator").build(); // 创建命名空间同时也限制监听范围
        curatorFramework.start();*/


        CuratorFramework client = CuratorWatcherDemo.createConnection("127.0.0.1:2181");

        addUnhandledErrorListenable(client);

        addConnectionStateListenable(client);

        client.start();


       // addListenerWithNodeCache(curatorFramework,"/ygm");

      //   addListenerWithPathChildCache(curatorFramework,"ygm");

      //  addListenerWithTreeCache(curatorFramework,"/ygm");
        addListenerWithTreeCache1(client,"/");

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        in.readLine();

    }

    /**
     * 创建连接
     * @param connectionString
     * @return
     */
    public static CuratorFramework createConnection(String connectionString)
    {
        // these are reasonable arguments for the ExponentialBackoffRetry. The first
        // retry will wait 1 second - the second will wait up to 2 seconds - the
        // third will wait up to 4 seconds.
        ExponentialBackoffRetry retryPolicy = new ExponentialBackoffRetry(1000, 3);

        // The simplest way to get a CuratorFramework instance. This will use default values.
        // The only required arguments are the connection string and the retry policy
        return CuratorFrameworkFactory.newClient(connectionString, retryPolicy);
    }

    /**
     * PathChildCache 监听一个节点下子节点的创建、删除、更新
     * NodeCache 监听一个节点的更新和创建事件
     * TreeCache 综合PatchChildCache和NodeCache的特性
     */

    /**
     * 子节点的增加、修改、删除的事件监听
     * @param curatorFramework
     * @param path
     */
    public static void addListenerWithPathChildCache(CuratorFramework curatorFramework,String path) throws Exception {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework,path,true);

        PathChildrenCacheListener pathChildrenCacheListener = new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                System.out.println("Receive Child Event:"+event.getType());
            }
        };

        pathChildrenCache.getListenable().addListener(pathChildrenCacheListener);
        pathChildrenCache.start(PathChildrenCache.StartMode.NORMAL);

    }

    /**
     * 当前节点的监听
     * @param curatorFramework
     * @param path
     * @throws Exception
     */
    public static void addListenerWithNodeCache(CuratorFramework curatorFramework,String path) throws Exception {
        NodeCache nodeCache = new NodeCache(curatorFramework,path,false);
        NodeCacheListener nodeCacheListener = new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                System.out.println("Receive Event:"+ nodeCache.getCurrentData().getPath());
            }
        };
        nodeCache.getListenable().addListener(nodeCacheListener);
        nodeCache.start();
    }

    /**
     * 综合监听事件(节点和子节点)
     * 写法一
     * @param curatorFramework
     * @param path
     * @throws Exception
     */
    public static void addListenerWithTreeCache(CuratorFramework curatorFramework,String path) throws Exception {
        TreeCache treeCache = new TreeCache(curatorFramework,path);
        TreeCacheListener treeCacheListener = new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                System.out.println(event.getType()+"->"+event.getData().getPath());
            }
        };
        treeCache.getListenable().addListener(treeCacheListener);
        treeCache.start();
    }

    /**
     * 综合监听事件(节点和子节点)
     * 写法二
     * @param curatorFramework
     * @param path
     * @throws Exception
     */
    public static void addListenerWithTreeCache1(CuratorFramework curatorFramework, String path) throws Exception {
        TreeCache treeCache = new TreeCache(curatorFramework,path);
        treeCache.getListenable().addListener((c,event)->{
            if(event.getData() != null){
                System.out.println("type="+ event.getType()+"  path"+ event.getData().getPath());
            }else {
                System.out.println("type="+event.getType());
            }

        });
        treeCache.start();
    }

    /**
     * 没有处理错误的监听
     * @param curatorFramework
     */
    public static void addUnhandledErrorListenable(CuratorFramework curatorFramework){
        curatorFramework.getUnhandledErrorListenable().addListener((message, e) -> {
            System.err.println("error=" + message);
            e.printStackTrace();
        });
    }

    /**
     * 连接状态的监听
     * @param curatorFramework
     */
    public static void addConnectionStateListenable(CuratorFramework curatorFramework){
        curatorFramework.getConnectionStateListenable().addListener((c, newState) -> {
            System.out.println("state=" + newState);
        });
    }


}
