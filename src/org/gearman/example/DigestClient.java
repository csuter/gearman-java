/*
 * Copyright (C) 2009 by Eric Herman <eric@freesa.org>
 * Use and distribution licensed under the
 * GNU Lesser General Public License (LGPL) version 2.1.
 * See the COPYING file in the parent directory for full text.
 */
package org.gearman.example;

import java.io.PrintStream;

import java.util.concurrent.Future;
import org.gearman.common.Constants;
import org.gearman.client.GearmanClient;
import org.gearman.client.GearmanClientImpl;
import org.gearman.client.GearmanJob;
import org.gearman.client.GearmanJobImpl;
import org.gearman.client.GearmanJobResult;
import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.gearman.util.ByteUtils;

public class DigestClient {

    private GearmanJobServerConnection conn;

    public DigestClient(GearmanJobServerConnection conn) {
        this.conn = conn;
    }

    public DigestClient(String host, int port) {
        this(new GearmanNIOJobServerConnection(host, port));
    }

    public byte[] digest(byte[] input) {
        String function = DigestFunction.class.getCanonicalName();
        String uniqueId = null;
        GearmanClient client = new GearmanClientImpl();
        client.addJobServer(conn);
        GearmanJob job = GearmanJobImpl.createJob(function, input, uniqueId);
        Future<GearmanJobResult> f = client.submit(job);
        GearmanJobResult jr = null;
        try {
            jr = f.get();
        } catch (Exception e) {
            e.printStackTrace();
        }
        if (jr == null) {
            return new byte[0];
        } else {
            return jr.jobSucceeded() ? jr.getResults() : jr.getExceptions();
        }
    }

    public static void main(String[] args) {
        if (args.length == 0 || args.length > 3) {
            usage(System.out);
            return;
        }
        String host = Constants.GEARMAN_DEFAULT_TCP_HOST;
        int port = Constants.GEARMAN_DEFAULT_TCP_PORT;
        byte[] payload = ByteUtils.toUTF8Bytes(args[args.length - 1]);
        for (String arg : args) {
            if (arg.startsWith("-h")) {
                host = arg.substring(2);
            } else if (arg.startsWith("-p")) {
                port = Integer.parseInt(arg.substring(2));
            }
        }
        byte[] md5 = new DigestClient(host, port).digest(payload);
        System.out.println(ByteUtils.toHex(md5));
    }

    public static void usage(PrintStream out) {
        String[] usage = {
            "usage: org.gearman.example.DigestClient [-h<host>] [-p<port>]" +
                    " <string>",
            "\t-h<host> - job server host",
            "\t-p<port> - job server port",
            "\n\tExample: java org.gearman.example.DigestClient Foo",
            "\tExample: java org.gearman.example.DigestClient -h127.0.0.1" +
                    " -p4730 Bar", //
        };

        for (String line : usage) {
            out.println(line);
        }
    }
}
