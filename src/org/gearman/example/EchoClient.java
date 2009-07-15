/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.example;

import java.io.IOException;
import org.gearman.common.Constants;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanPacketImpl;
import org.gearman.common.GearmanPacketMagic;
import org.gearman.common.GearmanPacketType;
import org.gearman.util.ByteUtils;
import java.io.PrintStream;
import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanNIOJobServerConnection;

public class EchoClient {

    private GearmanJobServerConnection connection;
    private boolean loop = true;

    public EchoClient(String host, int port) throws IOException {
        this.connection = new GearmanNIOJobServerConnection(host, port);
        connection.open();
    }

    public String echo(String input) throws IOException {
        byte[] data = ByteUtils.toAsciiBytes(input);
        GearmanPacket reverseRequest = new GearmanPacketImpl(
                GearmanPacketMagic.REQ, GearmanPacketType.ECHO_REQ, data);
        connection.write(reverseRequest);
        byte[] respBytes = readResponse();
        String echo = ByteUtils.fromAsciiBytes(respBytes);
        return echo;
    }

    private byte[] readResponse() throws IOException {
        byte[] respBytes = ByteUtils.EMPTY;
        while (loop) {
            GearmanPacket fromServer = connection.read();
            GearmanPacketType packetType = fromServer.getPacketType();
            if (packetType == GearmanPacketType.ECHO_RES) {
                return fromServer.getData();
            } else {
                println("Unexpected PacketType: " + packetType);
                println("Unexpected Packet: " + fromServer);
            }
        }
        return respBytes;
    }

    public void shutdown() {
        loop = false;
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length > 3) {
            usage(System.out);
            return;
        }
        String host = Constants.GEARMAN_DEFAULT_TCP_HOST;
        int port = Constants.GEARMAN_DEFAULT_TCP_PORT;
        String payload = args[args.length - 1];
        for (String arg : args) {
            if (arg.startsWith("-h")) {
                host = arg.substring(2);
            } else if (arg.startsWith("-p")) {
                port = Integer.parseInt(arg.substring(2));
            }
        }

        try {
            System.out.println(new EchoClient(host, port).echo(payload));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public static void usage(PrintStream out) {
        String[] usage = {
            "usage: gearmanij.example.EchoClient [-h<host>] [-p<port>] <string>",
            "\t-h<host> - job server host",
            "\t-p<port> - job server port",
            "\n\tExample: java gearmanij.example.EchoClient Foo",
            "\tExample: java gearmanij.example.EchoClient -h127.0.0.1" +
                    " -p4730 Bar", //
        };

        for (String line : usage) {
            out.println(line);
        }
    }

    public static void println(String msg) {
        System.err.println(Thread.currentThread().getName() + ": " + msg);
    }
}

