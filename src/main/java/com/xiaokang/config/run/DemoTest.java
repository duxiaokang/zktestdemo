package com.xiaokang.config.run;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author duxiaokang
 * @date 2017/12/1
 */
public class DemoTest {
    private static final Logger logger = LoggerFactory.getLogger(DemoTest.class);

    private static ZooKeeper zk;

    public static void main(String[] args) {
        // 创建/test ZNode。
        try {
            zk = new ZooKeeper("localhost:2181", 20000, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    try {
                        byte[] data = zk.getData("/test", true, null);
                        if (data == null) {
                            return;
                        }
                        System.out.println(new String(data, "UTF-8"));
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    }
                }
            });
            Stat stat = zk.exists("/test", true);
            if (stat == null) {
                zk.create("/test", null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                stat = zk.exists("/test", true);
            }
            zk.getData("/test", true, new AsyncCallback.DataCallback() {
                @Override
                public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                    System.out.println("rc:" + rc);
                    System.out.println("path:" + path);
                    System.out.println("ctx:" + ctx);
                    try {
                        System.out.println("data:" + new String(data, "UTF-8"));
                    } catch (Exception ignored) {
                    }
                    System.out.println("stat:" + stat);
                }
            }, null);
            zk.setData("/test", "杜小康".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "1".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "2".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "3".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "4".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "5".getBytes("UTF-8"), stat.getVersion());
            stat = zk.exists("/test", true);
            zk.setData("/test", "6".getBytes("UTF-8"), stat.getVersion());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        if (zk == null) {
            return;
        }

        try {
            Stat stat = zk.exists("/test", true);
            zk.delete("/test", stat.getVersion());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        try {
            zk.close();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
