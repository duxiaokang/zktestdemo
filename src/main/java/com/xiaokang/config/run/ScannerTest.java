package com.xiaokang.config.run;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Scanner;

/**
 * @author duxiaokang
 * @date 2017/12/12
 */
public class ScannerTest {
    private static final Logger logger = LoggerFactory.getLogger(ScannerTest.class);

    private static ZooKeeper zk;

    private static final String SERVER_LIST = "localhost:2181";

    private static final String ZNODE_1 = "/test";

    private static final int SESSION_TIMEOUT = 60 * 1000;

    public static void main(String[] args) throws Exception {
        // 创建/test ZNode。
        zk = new ZooKeeper(SERVER_LIST, SESSION_TIMEOUT, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                try {
                    Stat stat = zk.exists(ZNODE_1, false);
                    if (stat != null) {
                        zk.getData(ZNODE_1, false, new AsyncCallback.DataCallback() {
                            @Override
                            public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
                                System.out.println(rc);
                                System.out.println(path);
                                System.out.println(ctx);
                                System.out.println(new String(data));
                                System.out.println(stat);
                            }
                        }, event);
                    }
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                }
            }
        });
        System.out.println("");

        try {
            ioMethod();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        release();
    }

    private static void ioMethod() throws Exception {
        Scanner scanner = new Scanner(System.in);
        String line = scanner.nextLine();
        while (true) {
            if ("exit".equals(line)) {
                break;
            }

            if (line.startsWith("create")) {
                String[] str = line.split(" ");
                Stat stat = zk.exists(ZNODE_1, false);
                if (stat == null) {
                    String s = zk.create(ZNODE_1, str[1].getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                    System.out.println(s);
                }
            }

            if (line.startsWith("set")) {
                String[] str = line.split(" ");
                Stat stat = zk.exists(ZNODE_1, false);
                if (stat != null) {
                    zk.setData(ZNODE_1, str[1].getBytes(), stat.getVersion());
                } else {
                    System.out.println(ZNODE_1 + "节点不存在");
                }
            }

            if (line.startsWith("get")) {
                Stat stat = zk.exists(ZNODE_1, false);
                if (stat != null) {
                    byte[] data = zk.getData(ZNODE_1, false, stat);
                    if (data == null) {
                        System.out.println(ZNODE_1 + "节点上无数据");
                    } else {
                        String x = new String(data);
                        if ("".equals(x)) {
                            System.out.println(ZNODE_1 + "节点上无数据");
                        } else {
                            System.out.println(x);
                        }
                    }
                } else {
                    System.out.println(ZNODE_1 + "节点不存在");
                }
            }

            if (line.startsWith("ls")) {
                String[] str = line.split(" ");
                List<String> children;
                if (str.length == 1) {
                    children = zk.getChildren("/", false);
                } else {
                    children = zk.getChildren(str[1], false);
                }
                for (String s : children) {
                    System.out.println(s);
                }
            }

            line = scanner.nextLine();
        }
    }

    private static void release() {
        try {
            Stat stat = zk.exists(ZNODE_1, false);
            System.out.println(ZNODE_1 + "已删除");
            zk.delete(ZNODE_1, stat.getVersion());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

        try {
            System.out.println("Zookeeper正在断开连接...");
            zk.close();
            System.out.println("Zookeeper已断开连接");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
