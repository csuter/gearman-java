/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.common;

import junit.framework.Assert;
import org.gearman.client.GearmanEchoResponseHandler;
import org.gearman.common.GearmanTask.State;

import org.junit.Test;

public class GearmanServerRequestTest {

    @Test
    public void ctorTest() {
        GearmanEchoResponseHandler handler = new GearmanEchoResponseHandler();
        GearmanPacket request = new GearmanPacketImpl(GearmanPacketMagic.REQ, GearmanPacketType.ECHO_REQ,
                new byte[0]);
        GearmanPacket response = new GearmanPacketImpl(GearmanPacketMagic.RES, GearmanPacketType.ECHO_RES,
                new byte[0]);
        GearmanTask gsr = null;

        try {
            gsr = new GearmanTask(handler, null);
            Assert.fail("Attempt to create GearmanServerRequest with a null" +
                    " request did not result an exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("Attempt to create GearmanServerRequest with" +
                    " a null request resulted in an unexpected exception" +
                    " being raised", t instanceof IllegalArgumentException);
        }

        try {
            gsr = new GearmanTask(handler, response);
            Assert.fail("Attempt to create GearmanServerRequest with an" +
                    " invalid packet did not result an exception being raised");
        } catch (Throwable t) {
            Assert.assertTrue("Attempt to create GearmanServerRequest with" +
                    " an invalid packet resulted in an unexpected exception" +
                    " being raised", t instanceof IllegalArgumentException);
        }

        gsr = new GearmanTask(handler, request);
        Assert.assertEquals("Incorrect state for newly requested" +
                " GearmanServerRequest.", State.NEW, gsr.getState());
    }

    @Test
    public void toStringTest() {
        GearmanEchoResponseHandler handler = new GearmanEchoResponseHandler();
        GearmanPacket request = new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.ECHO_REQ, new byte[0]);
        GearmanTask gsr = new GearmanTask(handler, request);
        String s = gsr.toString();
        int firstDelim = s.indexOf(":");
        int lastDelim = s.lastIndexOf(":");
        Assert.assertTrue("String returned by toString (" + s + ") is not" +
                " in the expected format,", firstDelim > 0);
        Assert.assertTrue("String returned by toString (" + s + ") is not" +
                " in the expected format,", lastDelim != firstDelim);
        Assert.assertFalse("Unexpected suffix for String returned by toString",
                s.substring(lastDelim + 1).trim().equals(""));
        String expected  = GearmanTask.DESCRIPTION_PREFIX + ":" +
                handler.toString();
        String actual =  s.substring(0, lastDelim);
        Assert.assertEquals("First two tokens of string returned by toString" +
                " do not match expected format.", expected,actual);
    }

    @Test
    public void getHandlerTest() {
        GearmanEchoResponseHandler handler = new GearmanEchoResponseHandler();
        GearmanPacket request = new GearmanPacketImpl(GearmanPacketMagic.REQ,
                GearmanPacketType.ECHO_REQ, new byte[0]);
        GearmanTask gsr = new GearmanTask(handler, request);
        Assert.assertEquals("handler returned by getHandler does not match" +
                " handler passed into constructor.", handler, gsr.getHandler());
    }
}
