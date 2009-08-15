/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */

package org.gearman.worker;

import org.gearman.client.GearmanIOEventListener;
import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanPacketImpl;
import org.gearman.common.GearmanPacketMagic;
import org.gearman.common.GearmanPacketType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class AbstractGearmanFunctionTest {

    static class TestListener implements GearmanIOEventListener {

        GearmanPacketType t;
        boolean eventReceived = false;

        public TestListener(GearmanPacketType expected) {
            t = expected;
        }

        public void handleGearmanIOEvent(GearmanPacket event) throws IllegalArgumentException {
            if (event.getPacketType().equals(t)) {
                eventReceived = true;
            }
        }

        public boolean receivedEvent() {
            return eventReceived;
        }

    }
    
    static class TestFunction extends AbstractGearmanFunction {

        public TestFunction() {
            super();
        }
        
        public TestFunction(String name) {
            super(name);
        }

        @Override
        public GearmanJobResult executeFunction() {
            return new GearmanJobResultImpl(jobHandle, true, new byte[0],
                    new byte[0], new byte[0], 1, 2);
        }
        
    }

    static class TestBadFunction extends AbstractGearmanFunction {

        public TestBadFunction() {
            super();
        }

        public TestBadFunction(String name) {
            super(name);
        }

        @Override
        public GearmanJobResult executeFunction() {
            throw new IllegalArgumentException("BAH");
        }

    }

    TestFunction tf;
    
    @Before
    public void init () {
        tf = new TestFunction();
    }

    @Test
    public void ctorTest() {
        String testFunctionName = "aTestFunction";
        Assert.assertTrue(tf.getName().equals(tf.getClass().getCanonicalName()));

        tf = new TestFunction(testFunctionName);
        Assert.assertTrue(tf.getName().equals(testFunctionName));
    }

    @Test
    public void setHandleTest() {
        byte [] handle = {'d','e','a','d','b','e','e','f'};
        try {
            tf.setJobHandle(null);
            Assert.fail();
        } catch (IllegalArgumentException expected) {}

        try {
            tf.setJobHandle(new byte[0]);
            Assert.fail();
        } catch (IllegalArgumentException expected) {}

        tf.setJobHandle(handle);
        byte [] retrieveHandle = tf.getJobHandle();
        Assert.assertFalse(handle == retrieveHandle);
        Assert.assertArrayEquals(handle, retrieveHandle);
        retrieveHandle[0]=0;
        Assert.assertArrayEquals(handle, tf.getJobHandle());

    }

    @Test
    public void registerNullListenerTest() {
        try {
            tf.registerEventListener(null);
            Assert.fail();
        } catch (IllegalArgumentException expected) {}
    }

    @Test
    public void fireNullEventTest() {
        try {
            tf.fireEvent(null);
            Assert.fail();
        } catch (IllegalArgumentException expected) {}
    }

    @Test
    public void fireEventTest() {
        TestListener tl = new TestListener(GearmanPacketType.ERROR);
        tf.registerEventListener(tl);
        tf.fireEvent(new GearmanPacketImpl(GearmanPacketMagic.RES,
                GearmanPacketType.ERROR, new byte[0]));
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void sendDataTest() {
        TestListener tl = new TestListener(GearmanPacketType.WORK_DATA);
        tf.registerEventListener(tl);
        tf.sendData(new byte [] {'d','e','a','d'});
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void sendWarningTest() {
        TestListener tl = new TestListener(GearmanPacketType.WORK_WARNING);
        tf.registerEventListener(tl);
        tf.sendWarning(new byte[]{'d', 'e', 'a', 'd'});
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void sendExceptionTest() {
        TestListener tl = new TestListener(GearmanPacketType.WORK_EXCEPTION);
        tf.registerEventListener(tl);
        tf.sendException(new byte[]{'d', 'e', 'a', 'd'});
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void sendStatusTest() {
        TestListener tl = new TestListener(GearmanPacketType.WORK_STATUS);
        tf.registerEventListener(tl);
        tf.sendStatus(1, 2);
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void callTest () {
        TestListener tl = new TestListener(GearmanPacketType.WORK_COMPLETE);
        tf.registerEventListener(tl);
        tf.call();
        Assert.assertTrue(tl.eventReceived);
    }

    @Test
    public void badCallTest () {
        TestListener el = new TestListener(GearmanPacketType.WORK_EXCEPTION);
        TestListener fl = new TestListener(GearmanPacketType.WORK_FAIL);
        TestBadFunction tbf = new TestBadFunction();
        tbf.registerEventListener(el);
        tbf.registerEventListener(fl);
        tbf.call();
        Assert.assertTrue(el.receivedEvent());
        Assert.assertTrue(fl.receivedEvent());
    }
}
