package com.qiezi.taomo.net;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;

public class TaoMoServer implements Runnable {

    private static Selector selector;
    private static Logger LOG = LoggerFactory.getLogger(TaoMoServer.class);

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {

                System.out.println(t.getName() + e.getMessage());
            }
        });

        try {
            selector = Selector.open();
        } catch (IOException e) {
            LOG.error("io error:", e);
        }
    }

    private final String SERVER_NAME = "TAO-SERVER";
    private ServerSocketChannel ss;
    private int maxConn;
    private Map<InetAddress, Set<NIOServerCnxn>> connMap = new HashMap<>();

    public static void main(String[] args) {
        TaoMoServer server = new TaoMoServer();
        server.configure(9090, 1000);
    }

    public void configure(int listenPort, int maxConn) {
        try {
            ss = ServerSocketChannel.open();
            ss.configureBlocking(false);

            this.maxConn = maxConn;
            ss.socket().bind(new InetSocketAddress(listenPort));
            ss.register(selector, SelectionKey.OP_CONNECT);

            Thread thread = new Thread(this, "thread-" + SERVER_NAME);
            thread.start();
        } catch (IOException e) {
            LOG.error("error:", e);
        }

    }

    public void run() {
        while (!ss.socket().isClosed()) {
            try {
                selector.select(1000);
                Set<SelectionKey> keys = selector.selectedKeys();
                List<SelectionKey> keyList = new ArrayList<>(keys);
                Collections.shuffle(keyList);

                for (SelectionKey key : keyList) {
                    if ((key.interestOps() & SelectionKey.OP_ACCEPT) != 0) {
                        ServerSocketChannel ss = (ServerSocketChannel) key.channel();
                        SocketChannel sc = ss.accept();
                        InetAddress address = sc.socket().getInetAddress();

                        if (connMap.containsKey(address)) {
                            if (connMap.get(address).size() >= maxConn) {
                                LOG.info("too many connections... close the channel.");
                                sc.close();
                                continue;
                            }
                        }

                        sc.configureBlocking(false);
                        SelectionKey sk = sc.register(selector, SelectionKey.OP_READ);
                        NIOServerCnxn cnxn = new NIOServerCnxn(sc, sk);

                        sk.attach(cnxn);
                        addServerCnxn(cnxn);

                    } else if ((key.interestOps() & (SelectionKey.OP_READ | SelectionKey.OP_WRITE)) != 0) {
                        NIOServerCnxn serverCnxn = (NIOServerCnxn) key.attachment();
                        serverCnxn.doIO(key);
                    }
                }


            } catch (Exception e) {
                LOG.error("error :", e);
            }

        }
    }

    private void addServerCnxn(NIOServerCnxn cnxn) {
        InetAddress address = cnxn.getSc().socket().getInetAddress();
        synchronized (connMap) {
            if (connMap.containsKey(address)) {
                connMap.get(address).add(cnxn);
            } else {
                Set<NIOServerCnxn> cnxnSet = new HashSet<>();
                connMap.put(address, cnxnSet);
            }
        }

    }
}
