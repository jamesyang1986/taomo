package com.qiezi.taomo.net;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicLong;


public class NIOServerCnxn {

    private static final int SOCK_RECEIVE_BUF_SIZE = 1024 * 1024 * 4;
    private static final int SOCK_SEND_BUF_SIZE = 1024 * 1024 * 4;
    //send buffer for window size
    private final ByteBuffer directBuffer = ByteBuffer.allocateDirect(64 * 1024);
    private SocketChannel sc;
    private SelectionKey key;
    private Logger LOG = LoggerFactory.getLogger(NIOServerCnxn.class);
    private ByteBuffer headerBuffer = ByteBuffer.allocate(4);
    private ByteBuffer inComingBuffer = headerBuffer;
    private boolean isPayLoad = false;
    private LinkedBlockingQueue<ByteBuffer> outgoingBuffers = new LinkedBlockingQueue<>();
    private AtomicLong received = new AtomicLong(0L);
    private AtomicLong packetSend = new AtomicLong(0L);

    public NIOServerCnxn(SocketChannel sc, SelectionKey key) {
        this.sc = sc;
        this.key = key;
        try {
            sc.socket().setSendBufferSize(SOCK_SEND_BUF_SIZE);
            sc.socket().setReceiveBufferSize(SOCK_RECEIVE_BUF_SIZE);
            sc.socket().setKeepAlive(true);
            sc.socket().setReuseAddress(true);
            sc.socket().setSoLinger(false, -1);

            key.interestOps(key.interestOps() | SelectionKey.OP_READ);

        } catch (SocketException e) {
            LOG.error("error on io:", e);
        }

    }

    public void doIO(SelectionKey key) {
        if (key.isReadable()) {
            try {
                int rc = sc.read(inComingBuffer);
                if (rc < 0) {
                    LOG.info("read the end of sock stream... may be the client is closed");
                    sc.close();
                }

                if (inComingBuffer == headerBuffer) {
                    inComingBuffer.flip();
                    int payloadSize = inComingBuffer.getInt();
                    inComingBuffer = ByteBuffer.allocate(payloadSize);
                    inComingBuffer.clear();
                    isPayLoad = true;
                } else {
                    isPayLoad = true;
                }

                if (isPayLoad) {
                    readPayLoad();

                }
            } catch (IOException e) {
                LOG.error("error to read sock...", e);
            }


        } else if (key.isWritable()) {
            try {
                for (ByteBuffer bb : outgoingBuffers) {
                    if (bb.remaining() > directBuffer.remaining()) {
                        bb = (ByteBuffer) bb.slice().position(directBuffer.remaining());
                    }

                    //reset the source buffer position
                    int p = bb.position();
                    directBuffer.put(bb);
                    bb.position(p);

                    if (directBuffer.remaining() == 0) {
                        break;
                    }

                }


                int sent = sc.write(directBuffer);
                while (outgoingBuffers.size() > 0) {
                    ByteBuffer bb = outgoingBuffers.peek();

                    int left = bb.remaining() - sent;
                    if (left > 0) {
                        bb.position(bb.position() + sent);
                        break;
                    }

                    sent -= bb.remaining();
                    //finish to send one packet
                    packetSend.addAndGet(1);
                    outgoingBuffers.remove();

                }


                if (outgoingBuffers.size() == 0) {
                    key.interestOps(key.interestOps() & (~SelectionKey.OP_WRITE));
                } else {
                    key.interestOps(key.interestOps() | SelectionKey.OP_WRITE);
                }


            } catch (IOException e) {
                LOG.error("error for io:", e);
            }

        }

    }

    private void readPayLoad() {
        if (inComingBuffer.remaining() != 0) {
            try {
                int rc = sc.read(inComingBuffer);
                if (rc < 0) {
                    throw new RuntimeException("end of stream... may be the client is closed...");
                }

            } catch (IOException e) {

            }
        }

        if (inComingBuffer.remaining() == 0) {
            LOG.info("packet received....");
            received.addAndGet(1);

            inComingBuffer.flip();

            //pase the command from incoming buffer...
            handleCommand(inComingBuffer);

            //reset to receive next packet...
            headerBuffer.clear();
            inComingBuffer = headerBuffer;
            isPayLoad = false;
        }


    }

    private void handleCommand(ByteBuffer inComingBuffer) {

        //test code, to fix
        ByteBuffer sendBuf = ByteBuffer.allocate(4 + 4 + 8 + 8);
        sendBuf.putInt(0xCAFFEEDD);
        sendBuf.putInt(0);
        sendBuf.putLong(111L);
        sendBuf.putLong(2222L);

        outgoingBuffers.add(sendBuf);

    }

    public SocketChannel getSc() {
        return sc;
    }

    public void setSc(SocketChannel sc) {
        this.sc = sc;
    }

    public SelectionKey getKey() {
        return key;
    }

    public void setKey(SelectionKey key) {
        this.key = key;
    }
}
