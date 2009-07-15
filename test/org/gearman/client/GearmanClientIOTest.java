/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import java.io.IOException;
import java.util.Arrays;

import junit.framework.Assert;

import org.gearman.common.GearmanNIOJobServerConnection;
import org.junit.Before;
import org.junit.Test;

//TODO make this MUCH more usefull
public class GearmanClientIOTest {

    GearmanClientImpl gc = new GearmanClientImpl();

    @Before
    public void initClient() throws IOException {
        gc.addJobServer(new GearmanNIOJobServerConnection("127.0.0.1"));
    }

    @Test
    public void echoTest() throws IOException {
        byte[] data = {'D', 'E', 'A', 'D', 'B', 'E', 'E', 'F'};
        Assert.assertTrue(Arrays.equals(data, gc.echo(data)));
    }
}
