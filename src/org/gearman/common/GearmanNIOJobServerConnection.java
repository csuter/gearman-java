/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.common;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

public class GearmanNIOJobServerConnection
        implements GearmanJobServerConnection {

    static final String DESCRIPTION_PREFIX = "GearmanNIOJobServerConnection";
    private final String DESCRIPTION;
    private InetSocketAddress remote;
    private SocketChannel serverConnection = null;
    private Selector selector = null;
    private SelectionKey selectorKey = null;
    private static final Logger LOG = Logger.getLogger(
            Constants.GEARMAN_CLIENT_LOGGER_NAME);
    private ByteBuffer bytesReceived;
    private ByteBuffer bytesToSend;

    public GearmanNIOJobServerConnection(String hostname)
            throws IllegalArgumentException {
        this(hostname, Constants.GEARMAN_DEFAULT_TCP_PORT);
    }

    public GearmanNIOJobServerConnection(String hostname, int port)
            throws IllegalArgumentException {
        this(new InetSocketAddress(hostname, port));
    }

    public GearmanNIOJobServerConnection(InetSocketAddress remote)
            throws IllegalArgumentException {
        if (remote == null) {
            throw new IllegalArgumentException("Remote can not be null");
        }
        this.remote = remote;
        bytesReceived = ByteBuffer.allocate(
                Constants.GEARMAN_DEFAULT_SOCKET_RECV_SIZE);
        bytesToSend = ByteBuffer.allocate(
                Constants.GEARMAN_DEFAULT_SOCKET_RECV_SIZE);
        DESCRIPTION = DESCRIPTION_PREFIX + ":" + remote.toString();
    }

    @Override
    public String toString () {
        return DESCRIPTION;
    }

    public void open() throws IOException {
        if (isOpen()) {
            throw new IllegalStateException("A session can not be " +
                    "initialized twice");
        }
        try {
            serverConnection = SocketChannel.open(remote);
            serverConnection.configureBlocking(false);
            serverConnection.finishConnect();
            selector = Selector.open();
            selectorKey = serverConnection.register(selector,
                    SelectionKey.OP_WRITE | SelectionKey.OP_READ);
        } catch (IOException ioe) {
            LOG.log(Level.WARNING, "Received IOException while attempting to" +
                    " initialize session " + this +
                    ". Shuting down session", ioe);
            if (serverConnection != null && serverConnection.isOpen()) {
                if (selector != null && selector.isOpen()) {
                    try {
                        selector.close();
                    } catch (IOException selioe) {
                        LOG.log(Level.WARNING, "Received IOException while" +
                                " attempting to close selector.", selioe);
                    }
                }
                try {
                    serverConnection.close();
                } catch (IOException closeioe) {
                    LOG.log(Level.WARNING, "Received IOException while" +
                            " attempting to close connection to server. " +
                            "Giving up!", closeioe);
                }
            }
            throw new IOException();
        }
        LOG.log(Level.FINE,"Connection " + this + " has been opened");
    }

    public void close() {
        if (!isOpen()) {
            throw new IllegalStateException("Can not close a session that " +
                    "has not been initialized");
        }
        LOG.log(Level.FINE, "Session " + this + " is being closed.");
        selectorKey.cancel();
        try {
            selector.close();
        } catch (IOException ioe) {
            LOG.log(Level.WARNING, "Received IOException while attempting to " +
                    "close selector attached to session " + this, ioe);
        } finally {
            try {
                serverConnection.close();
            } catch (IOException cioe) {
                LOG.log(Level.WARNING, "Received IOException while attempting" +
                        " to close connection for session " + this, cioe);
            }
            serverConnection = null;
        }
        LOG.log(Level.FINE, "Connection " + this + " has successfully closed.");
    }

    public void write(GearmanPacket request) throws IOException {

        if (request == null && bytesToSend.position() == 0) {
            return;
        }
        if (request != null) {
            int ps = request.getData().length +
                    Constants.GEARMAN_PACKET_HEADER_SIZE;
            if (bytesToSend.remaining() < ps) {
                //TODO allocate more
                ByteBuffer bb = ByteBuffer.allocate(bytesToSend.capacity() +
                        (ps * 10));
                bb.put(bytesToSend);
                bytesToSend = bb;
            }
            byte[] bytes = request.toBytes();
            ByteBuffer bb = ByteBuffer.allocate(bytes.length);
            bb.put(bytes);
            bb.rewind();
            bytesToSend.put(bb);
        }
        selector.selectNow();
        if (selectorKey.isWritable()) {
            bytesToSend.limit(bytesToSend.position());
            bytesToSend.rewind();
            int bytesSent = serverConnection.write(bytesToSend);
            bytesToSend.compact();
            LOG.log(Level.FINER,"Write command wrote " + bytesSent + " to " +
                    this + ". " + bytesToSend.position() + " bytes left in " +
                    "send buffer");
        } else {
            LOG.log(Level.FINER,"Write command can not write request: " +
                    "Selector for " + this + " is not available for write. " +
                    "Will buffer request and send it later.");
        }
    }

    public GearmanPacket read() throws IOException {
        GearmanPacket returnPacket = null;
        selector.selectNow();
        if (selectorKey.isReadable()) {
            int bytesRead = serverConnection.read(bytesReceived);
            if (bytesRead >= 0) {
                LOG.log(Level.FINER, "Session " + this + " has read " +
                        bytesRead + " bytes from its job server. Buffer has " +
                        bytesReceived.remaining());
            } else {
                //TODO do something smarter here
                throw new IOException("Connection to job server severed");
            }
        } else {
            LOG.log(Level.FINER,"Read command can not read request from" +
                    "session: Selector for " + this + " is not available " +
                    "for read. ");
        }
        if (bufferContainsCompletePacket(bytesReceived)) {
            byte[] pb = new byte[getSizeOfPacket(bytesReceived)];
            bytesReceived.limit(bytesReceived.position());
            bytesReceived.rewind();
            bytesReceived.get(pb);
            bytesReceived.compact();
            returnPacket = new GearmanPacketImpl(new BufferedInputStream(
                    new ByteArrayInputStream(pb)));
        }
        return returnPacket;
    }

    public SelectionKey registerSelector(Selector s, int mask)
            throws IOException {
        return serverConnection.register(s, mask);

    }

    public boolean canRead() {
        if (!selector.isOpen()) {
            return false;
        }
        try {
            selector.selectNow();
        } catch (IOException ioe) {
            LOG.log(Level.WARNING, "Failed to select on connection " +
                    this, ioe);
        }
        return (selectorKey.isReadable() ||
                bufferContainsCompletePacket(bytesReceived));
    }

    public boolean canWrite() {
        if (!selector.isOpen()) {
            return false;
        }
        try {
            selector.selectNow();
        } catch (IOException ioe) {
            LOG.log(Level.WARNING, "Connection Failed to select on socket " +
                    this, ioe);
        }
        return (selectorKey.isWritable());
    }

    public boolean hasBufferedWriteData() {
        return bytesToSend.position() > 0;
    }

    public Selector getSelector() {
        return selector;
    }

    public boolean isOpen() {
        return (serverConnection != null && serverConnection.isConnected());
    }

    @Override
    public boolean equals(Object that) {
        if (that == null) {
            return false;
        }

        if (this == that) {
            return true;
        }

        if (!(that instanceof GearmanNIOJobServerConnection)) {
            return false;
        }

        InetSocketAddress thatRemote = 
                ((GearmanNIOJobServerConnection) that).remote;

        return this.remote.equals(thatRemote);
    }

    // When you override equals you should override hashcode as well, since
    // two equal objects should have the same hashcode
    @Override
    public int hashCode() {
        return this.remote != null ? this.remote.hashCode() : 0;
    }

    private boolean bufferContainsCompletePacket(ByteBuffer b) {
        if (b.position() < Constants.GEARMAN_PACKET_HEADER_SIZE) {
            return false;
        }
        return b.position() >= getSizeOfPacket(b) ? true : false;
    }

    // DO NOT CALL UNLESS YOU ARE SURE THAT BYTEBUFFER HAS AT LEAST
    // GEARMAN_PACKET_HEADER_SIZE BYTES!
    private int getSizeOfPacket(ByteBuffer buffer) {
        int originalPosition = buffer.position();
        byte[] header = new byte[Constants.GEARMAN_PACKET_HEADER_SIZE];
        buffer.rewind();
        buffer.get(header);
        buffer.position(originalPosition);
        GearmanPacketHeader ph = new GearmanPacketHeader(header);
        return ph.getDataLength() + Constants.GEARMAN_PACKET_HEADER_SIZE;
    }
}
