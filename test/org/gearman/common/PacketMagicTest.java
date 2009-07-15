/*
 * Copyright (C) 2009 by Eric Herman <eric@freesa.org>
 * Use and distribution licensed under the 
 * GNU Lesser General Public License (LGPL) version 2.1.
 * See the COPYING file in the parent directory for full text.
 */
package org.gearman.common;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.gearman.common.GearmanPacketMagic.BadMagicException;
import org.junit.Test;

public class PacketMagicTest {
    byte[] reqBytes = new byte[] { 0, 'R', 'E', 'Q' };
    byte[] resBytes = new byte[] { 0, 'R', 'E', 'S' };

    @Test
    public void testMagicBytes() {
        assertEquals(2, GearmanPacketMagic.values().length);
        assertTrue(Arrays.equals(reqBytes, GearmanPacketMagic.REQ.toBytes()));
        assertTrue(Arrays.equals(resBytes, GearmanPacketMagic.RES.toBytes()));
    }

    @Test
    public void testFromBytes() {
        /*
         * "\0REQ" == [ 00 52 45 51 ] == 5391697
         * 
         * "\0RES" == [ 00 52 45 53 ] == 5391699
         */
        assertEquals(GearmanPacketMagic.REQ, GearmanPacketMagic.fromBytes(reqBytes));
        assertEquals(GearmanPacketMagic.RES, GearmanPacketMagic.fromBytes(resBytes));
        BadMagicException expected = null;
        try {
            GearmanPacketMagic.fromBytes(new byte[] { 2, 3, 4, 5 });
        } catch (BadMagicException e) {
            expected = e;
        }
        assertNotNull(expected);
    }
}
