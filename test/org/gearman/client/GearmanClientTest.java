/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.ServerSocket;
import java.util.Collection;

import org.gearman.common.Constants;
import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanNIOJobServerConnection;

import org.junit.Ignore;
import org.junit.Test;

import junit.framework.Assert;


public class GearmanClientTest {

    @Test
    /*
     * Basic constructor test. Ensures that
     * a new constructed client
     * -is active
     * -has no sessions
     */
    public void CtorTest() {
        GearmanClientImpl gc = new GearmanClientImpl();
        Assert.assertFalse("Newly constructed client is not active.",
                gc.isShutdown());
        Assert.assertTrue("Newly constructed client has no sessions",
                gc.getSetOfJobServers().size() == 0);
    }

    @Test
    /*
     * Basic add host test. Ensures
     * -that I can add a unique host to the client
     * -that calling hasJobServer before adding it returns false
     * -that calling hasJobServer after adding it returns true
     * -that the number of JobServers the client has is equal to the
     *  number that has been added.
     * -verify that when we ask for the servers in the client we get them all
     */
    public void addHostsTests() throws IOException {
        GearmanClientImpl gc = new GearmanClientImpl();
        int port = Constants.GEARMAN_DEFAULT_TCP_PORT + 1;
        int iters = 10;
        for (int i = 0; i < iters; i++) {
            ServerSocket s = new ServerSocket(port + i);
            GearmanNIOJobServerConnection conn =
                    new GearmanNIOJobServerConnection("localhost", port + i);
            Assert.assertFalse("Client claims to have jobserver localhost:" +
                    port + " before it was added",
                    gc.hasConnection(conn));
            gc.addJobServer(conn);
            Assert.assertTrue("Client claims not to have jobserver localhost:" +
                    port + " after it was added",
                    gc.hasConnection(conn));
            Assert.assertTrue("Number of jobservers in client does not equal" +
                    " number that has been added.",
                    gc.getSetOfJobServers().size() == i + 1);
            s.close();
        }
        Collection<GearmanJobServerConnection> listback = gc.getSetOfJobServers();
        Assert.assertTrue("Number of jobServers returned doesn't equal number" +
                " added", listback.size() == iters);
        for (int i = 0; i < iters; i++) {
            Assert.assertTrue("List of jobServers returned doesn't contain" +
                    " server for port " + port + i,
                    listback.contains(
                    new GearmanNIOJobServerConnection("localhost", port + i)));
        }
    }

    @Test
    /*
     * Attempt to use invalid host and port number as jobServers.
     * All cases should result in a IllegalArgumentException
     */
    public void addBadServer() throws IllegalStateException, IOException {
        //invalid host
        GearmanClientImpl gc = new GearmanClientImpl();
        try {
            gc.addJobServer(null);
            Assert.fail("Was able to add a null job server to the client.");
        } catch (IllegalArgumentException expected) {
        }
    }

    @Test
    /*
     * Basic dup add test
     * -Ensures that when we try to add a duplicate jobserver, we silently
     * ignore the add and dont increase the number of sessions
     */
    public void addDupJobServer() throws IOException {
        GearmanClientImpl gc = new GearmanClientImpl();
        GearmanNIOJobServerConnection conn =
                    new GearmanNIOJobServerConnection("localhost",
                    Constants.GEARMAN_DEFAULT_TCP_PORT);
        gc.addJobServer(conn);
        int numberOfServers = gc.getSetOfJobServers().size();
        gc.addJobServer(conn);
        Assert.assertTrue("Adding a duplicate jobServer to a client changed" +
                " the number of jobServer in the client.",
                gc.getSetOfJobServers().size() == numberOfServers);
    }

    @Test
    /*
     * Basic shutdown test
     *-Ensure that I can shut down and that afterwards the client is not active
     */
    public void basicShutdownTest() throws IOException {
        GearmanClientImpl gc = new GearmanClientImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));
        gc.shutdown();
        Assert.assertTrue("Client is active after attempting to shutdown" +
                " client", gc.isShutdown());
    }

    @Test
    /*
     * Adds a series of job servers to a client and then removes them, verifying
     * that the number or servers is decremented as we do remove them
     */
    public void removeJobServerTest() throws IOException {
        GearmanClientImpl gc = new GearmanClientImpl();
        int port = Constants.GEARMAN_DEFAULT_TCP_PORT + 1;
        int iters = 10;
        ServerSocket[] ss = new ServerSocket[iters];
        for (int i = 0; i < iters; i++) {
            ss[i] = new ServerSocket(port + i);
            gc.addJobServer(
                    new GearmanNIOJobServerConnection("localhost", port + i));
        }
        for (int i = 0; i < iters; i++) {
            gc.removeJobServer(
                    new GearmanNIOJobServerConnection("localhost", port + i));
            Assert.assertTrue("removeJobServer call failed to remove server" +
                    " from client.",
                    gc.getSetOfJobServers().size() == iters - i - 1);
            ss[i].close();
        }
    }

    @Test
    /*
     * Attempt to remove a job server that has not been registered with the
     * client
     * verify that
     * -remove attempt results in exception
     * -no job server have been removed
     *
     */
    public void removeNonExistantJobServer() throws IOException {
        GearmanClientImpl gc = new GearmanClientImpl();
        int port = Constants.GEARMAN_DEFAULT_TCP_PORT;
        ServerSocket s = new ServerSocket(port + 1);
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost", port + 1));
        try {
            gc.removeJobServer(
                    new GearmanNIOJobServerConnection("localhost", port - 1));
            Assert.fail("Attempt to remove a non-registered job server should" +
                    " have thrown exception but no exception was thrown");
        } catch (IllegalArgumentException expected) {
        }
        Assert.assertTrue("JobServer set size was modified after attempt to" +
                " remove a non-registered job server",
                gc.getSetOfJobServers().size() == 1);

    }

    @Ignore
    /*
     * Shutdown method invocation test.
     * -Ensures that after a client has been shut down, any attempt to invoke
      * a public method on object
     *  throws a GearmanClient.ClientShutdown exception
     */
    /** TEST IS BROKEN, throws some god awful exception when trying to invoke
     * hasJobServers, I sure problem is related ignorance of reflection */
    public void invokeShutdownTest() throws IOException,IllegalAccessException,
            InvocationTargetException {
        GearmanClientImpl gc = new GearmanClientImpl();
        gc.addJobServer(new GearmanNIOJobServerConnection("localhost"));
        gc.shutdown();
        Method m[] = gc.getClass().getDeclaredMethods();
        for (int i = 0; i < m.length; i++) {
            if (m[i].getModifiers() != Modifier.PUBLIC ||
                    m[i].getName().equals("shutdown") ||
                    m[i].getName().equals("isActive")) {
                continue;
            }
            Object[] arglist = new Object[m[i].getGenericParameterTypes().length];
            for (int x = 0; x < arglist.length; x++) {
                arglist[x] = null;
            }
            System.out.println("Trying " + m[i].getName());
            try {
                m[i].invoke(gc, arglist);
                Assert.fail("Attempt to invoke method " + m[i] + " on a" +
                        " shutdown client did not result in an exception" +
                        " being raised");
            } catch (InvocationTargetException ite) {
                Throwable t = ite.getCause();
                Assert.assertTrue("Attempt to invoke method " + m[i] + " on" +
                        " a shutdown client did not result in a " +
                        IllegalStateException.class + " exception" +
                        " being raised",
                        t instanceof IllegalStateException);
            }
        }
    }
}
