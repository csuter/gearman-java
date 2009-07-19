/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.common;


import java.io.IOException;
import java.net.ServerSocket;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;

import junit.framework.Assert;

import org.gearman.client.GearmanEchoResponseHandler;

import org.junit.Test;

public class GearmanJobServerSessionTest {

    @Test
    public void ctorTest() {
        GearmanNIOJobServerConnection localhost = new
                GearmanNIOJobServerConnection("localhost");
        GearmanJobServerSession session = new GearmanJobServerSession(localhost);
        Assert.assertFalse("session incorrectly claims to be initialized",
                session.isInitialized());
        Assert.assertEquals(GearmanJobServerSession.DESCRIPTION_PREFIX + ":" +
                Thread.currentThread().getId() + ":" +
                localhost, session.toString());
        Throwable thrown = null;
        try {
            session = new GearmanJobServerSession(null);
            Assert.fail("No exception was thrown on attempt to create a new" +
                    " sesssion with a null remote address");
        } catch (Throwable t) {
            thrown = t;
        }
        Assert.assertTrue("Unexpected exception was thrown on attempt to" +
                " create a new sesssion with a null remote address",
                thrown instanceof IllegalArgumentException);
    }

    @Test
    public void getConnectionTest() {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost");
        GearmanJobServerSession session = new GearmanJobServerSession(conn);
        Assert.assertEquals("session getConnection return value does not equal" +
                " server used to create session",conn, session.getConnection());
    }

    @Test
    public void initializeSessionTest() throws IOException {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        GearmanJobServerSession session = null;
        //try and initialize using a remote that we can not connect to
        session = new GearmanJobServerSession(conn);
        try {
            session.initSession(Selector.open(),null);
            Assert.fail("initSession call did not throw exception even " +
                    "though session can not connect to host");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when initializing" +
                    " session with a non-connectable host. Expected  " +
                    "IOException, got " + t, t instanceof IOException);
        }

        ServerSocket s = new ServerSocket(Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        session.initSession(Selector.open(),null);
        try {
            session.initSession(Selector.open(),null);
            Assert.fail("initSession was called twice but did throw " +
                    "exception on second call");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when making " +
                    "duplicate initialize session call. " +
                    "Expected IllegalStateException, got " + t,
                    t instanceof IllegalStateException);
        }
        session.closeSession();
        s.close();
    }

    @Test
    public void closeSessionTest() throws IOException {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        GearmanJobServerSession session = null;
        session = new GearmanJobServerSession(conn);
        ServerSocket s = new ServerSocket(
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        try {
            session.closeSession();
            Assert.fail("Attempt to close non-initialized session did not " +
                    "result an exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when attempting" +
                    " to close non-initialized session . " +
                    "Expected IllegalStateException, got " + t,
                    t instanceof IllegalStateException);
        }

        session.initSession(Selector.open(),null);
        session.closeSession();
        try {
            session.closeSession();
            Assert.fail("Duplicate close session did not result an" +
                    " exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when making" +
                    " duplicate initialize session call. " +
                    "Expected IllegalStateExceptio, got " + t,
                    t instanceof IllegalStateException);

        }
        s.close();
    }

    @Test
    public void getKeyForSelectorTest() throws IOException {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        GearmanJobServerSession session = null;
        session = new GearmanJobServerSession(conn);
        try {
            session.getSelectionKey();
            Assert.fail("Attempt to obtain selectKeys for non-initialized" +
                    " session did not result an exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when attempting" +
                    " to obtain selectKeys for non-initialized session. " +
                    "Expected IllegalStateException, got " + t,
                    t instanceof IllegalStateException);
        }

        ServerSocket s = new ServerSocket(
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        session.initSession(Selector.open(),null);
        Assert.assertTrue(session.getSelectionKey() != null);
        try {
            SelectionKey key = session.getSelectionKey();
            Assert.assertTrue(key != null);
        } finally {
            session.closeSession();
            s.close();
        }
    }

    @Test
    public void submitRequestToSessionTest() throws IOException {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        GearmanJobServerSession session = null;
        session = new GearmanJobServerSession(conn);
        GearmanPacket echoRequest = new GearmanPacketImpl(
                GearmanPacketMagic.REQ, GearmanPacketType.ECHO_REQ, new byte[0]);
        GearmanTask gsr = new GearmanTask(
                new GearmanEchoResponseHandler(), echoRequest);
        ServerSocket s = null;

        try {
            session.submitTask(gsr);
            Assert.fail("Attempt to submit request to non-initialized " +
                    "session did not result an exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("unexpected exception thrown when attempting to" +
                    " submit request to non-initialized session. " +
                    "Expected IllegalStateException, got " + t,
                    t instanceof IllegalStateException);
        }

        try {
            s = new ServerSocket(Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
            session.initSession(Selector.open(),null);

            try {
                session.submitTask(null);
                Assert.fail("Attempt to submit null request to session did" +
                        " not result an exception being raised");
            } catch (Throwable t) {
                Assert.assertTrue("unexpected exception thrown when" +
                        " attempting to submit null request session. " +
                        "Expected IllegalStateException, got " + t,
                        t instanceof IllegalStateException);
            }

            try {
                session.submitTask(gsr);
            } catch (Throwable t) {
                Assert.fail("unexpected exception thrown when attempting" +
                        " to submit valied request session: " + t);
            }
        } finally {
            try {
                session.closeSession();
                s.close();
            } catch (Exception swallow) {
            }
        }
    }

    @Test
    public void getNumberOfActiveTasksTest() throws IOException {
        GearmanNIOJobServerConnection conn =
                new GearmanNIOJobServerConnection("localhost",
                Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
        GearmanJobServerSession session = null;
        session = new GearmanJobServerSession(conn);
        GearmanPacket echoRequest = new GearmanPacketImpl(
                GearmanPacketMagic.REQ, GearmanPacketType.ECHO_REQ, new byte[0]);
        GearmanTask gsr = new GearmanTask(
                new GearmanEchoResponseHandler(), echoRequest);
        ServerSocket s = null;

        boolean exceptionThrown = false;
        try {
            session.getNumberOfActiveTasks();
        } catch (IllegalStateException ise) {
            exceptionThrown = true;
        }
        Assert.assertTrue("Calling getNumberOfActiveTasks on un-initialized" +
                " session did not result in IllegalStateException",
                exceptionThrown);
        try {
            s = new ServerSocket(Constants.GEARMAN_DEFAULT_TCP_PORT - 1);
            session.initSession(Selector.open(),null);
            session.submitTask(gsr);
            Assert.assertEquals("getNumberOfActiveTasks mis-matchs ",1,
                    session.getNumberOfActiveTasks());
        } finally {
            try {
                session.closeSession();
                s.close();
            } catch (Exception swallow) {
            }
        }
    }
}
