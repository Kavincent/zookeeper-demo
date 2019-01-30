package com.ygm.zookerper;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Created by admin on 2019/1/21.
 */
public class WatcherDemo {
    public static void main(String[] args) {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        try {
            ZooKeeper zooKeeper = new ZooKeeper("127.0.0.1", 4000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println("默认事件:"+event.getType());
                    if (Event.KeeperState.SyncConnected == event.getState()){
                        // 如果收到了服务端的响应事件，连接成功
                        countDownLatch.countDown();
                    }
                }
            });
            countDownLatch.await();
            zooKeeper.create("/zk-persis-ygm","1".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            // exists getdata getchildren
            // 通过exists绑定事件
            Stat stat = zooKeeper.exists("/zk-persis-ygm", new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    System.out.println(event.getType() + "->" + event.getPath());
                    try {
                        // 再一次绑定的事件
                        zooKeeper.exists(event.getPath(),true);
                    } catch (KeeperException e) {
                        e.printStackTrace();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            });

            // 通过修改的事物类型操作来触发监听事件
            stat = zooKeeper.setData("/zk-persis-ygm", "2".getBytes(), stat.getVersion());


            Thread.sleep(1000);

            zooKeeper.delete("/zk-persis-ygm",stat.getVersion());

            System.in.read();

            zooKeeper.close();

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }

    }
}
