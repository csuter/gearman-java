/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.util;

import org.gearman.client.GearmanIOEventListener;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanPacketType;
import org.gearman.util.ByteUtils;

public class IncrementalListener  implements GearmanIOEventListener {

    private StringBuffer sb = new StringBuffer();

    public void handleGearmanIOEvent(GearmanPacket event)
            throws IllegalArgumentException {
        if (!event.getPacketType().equals(GearmanPacketType.WORK_DATA)) {
            return;
        }
        sb.append(ByteUtils.fromUTF8Bytes((event.getDataComponentValue(
                GearmanPacket.DataComponentName.DATA))));
    }

    public String getResults() {
        return sb.toString();
    }
}
