/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */

package org.gearman.worker;

import org.gearman.common.Constants;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.Set;
import javax.imageio.IIOException;
import org.gearman.common.GearmanPacket;
import org.gearman.client.GearmanIOEventListener;
import org.junit.Test;
import junit.framework.Assert;
import org.gearman.client.GearmanClient;
import org.gearman.common.GearmanJobServerConnection;
import org.gearman.common.GearmanNIOJobServerConnection;
import org.junit.After;
import org.junit.Before;

public class GearmanWorkerImplTest {

    GearmanWorkerImpl worker;
    ServerSocket s = null;
    ServerSocket s1 = null;

    @Before
    public void init () {
        worker = new GearmanWorkerImpl();
        //Create a dummy socket so that some tests have something to connect
        //to.  Assumes that IOException means a GearmanServer is already running
        //on that port, so just ignore the exception.
        try {
            s = new ServerSocket(Constants.GEARMAN_DEFAULT_TCP_PORT  + 1);
        } catch (IOException ignore) {
            System.out.println(ignore);
        }
    }

    @After
    public void shutdown() {
        try {
            if (s != null) {
                s.close();
            }
            if (s1 != null) {
                s1.close();
            }
        } catch (IOException ioe) {}
    }

    @Test
    public void ctorTest() {
        Assert.assertFalse(worker.isRunning());
        Assert.assertTrue(worker.getRegisteredFunctions().isEmpty());
        Assert.assertEquals("GearmanWorker:" + Thread.currentThread().getId(),
                worker.getWorkerID());
    }

    @Test
    public void registerFunctionTest() {
        worker.registerFunction(ExampleFunction.class.getCanonicalName());
        Set<String> regFuncs = worker.getRegisteredFunctions();
        Assert.assertTrue(regFuncs.size() == 1);
        Assert.assertTrue(regFuncs.contains(ExampleFunction.class.getCanonicalName()));
    }

    @Test
    public void registerFunctionTestWithTimeout() {
        worker.registerFunction(ExampleFunction.class.getCanonicalName(),2000);
        Set<String> regFuncs = worker.getRegisteredFunctions();
        Assert.assertTrue(regFuncs.size() == 1);
        Assert.assertTrue(regFuncs.contains(ExampleFunction.class.getCanonicalName()));
    }

    @Test
    public void registerDupFunctionTest() {
        worker.registerFunction(ExampleFunction.class.getCanonicalName());
        worker.registerFunction(ExampleFunction.class.getCanonicalName());
        Set<String> regFuncs = worker.getRegisteredFunctions();
        Assert.assertTrue(regFuncs.size() == 1);
        Assert.assertTrue(regFuncs.contains(ExampleFunction.class.getCanonicalName()));
    }

    @Test
    public void unregisterFunctionTest() {
        worker.registerFunction(ExampleFunction.class.getCanonicalName());
        worker.unregisterFunction(ExampleFunction.class.getCanonicalName());
        Set<String> regFuncs = worker.getRegisteredFunctions();
        Assert.assertTrue(regFuncs.size() == 0);
    }

    @Test
    public void unregisterAllFunctionTest() {
        worker.registerFunction(ExampleFunction.class.getCanonicalName());
        worker.unregisterAll();
        Set<String> regFuncs = worker.getRegisteredFunctions();
        Assert.assertTrue(regFuncs.size() == 0);
    }

    @Test
    public void setWorkerIDTest () {
        String id = "how now brown cow";
        worker.setWorkerID(id);
        Assert.assertEquals(id, worker.getWorkerID());
        try {
            worker.setWorkerID(null);
            Assert.fail("Setting workerID to null value did not throw exception");
        } catch (IllegalArgumentException expected) {}
        Assert.assertEquals(id, worker.getWorkerID());
    }

    @Test
    public void addServerTest() {
        GearmanNIOJobServerConnection conn = new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT + 1);
        Assert.assertTrue(worker.addServer(conn));
        Assert.assertTrue((worker.hasServer(conn)));
    }

    @Test
    public void addDupServerTest() {
        GearmanNIOJobServerConnection conn = new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT + 1);
        Assert.assertTrue(worker.addServer(conn));
        Assert.assertTrue((worker.hasServer(conn)));
        
        // For the time being, we define success as the dup 'add' not blowing up
        // and that the server is still known to the client. Ideally, we should
        // check and make sure that we have not increased the number of servers
        // known by the worker, but the test lacks the visibility to test that.
        conn = new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT + 1);
        Assert.assertTrue(worker.addServer(conn));
        Assert.assertTrue((worker.hasServer(conn)));
    }

    @Test
    public void addNullServerTest() {
        try {
            worker.addServer(null);
            Assert.fail();
        } catch (IllegalArgumentException expected) {}
    }

}
