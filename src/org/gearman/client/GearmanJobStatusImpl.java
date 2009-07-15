/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.client;

import org.gearman.common.GearmanException;
import org.gearman.util.ByteUtils;
import org.gearman.common.GearmanPacketType;
import org.gearman.common.GearmanPacket;
import org.gearman.common.GearmanServerResponseHandler;

public class GearmanJobStatusImpl implements GearmanServerResponseHandler,
        GearmanJobStatus {

    private static final String DESCRIPTION = "GearmanJobStatus";
    private boolean isRunning = false;
    private boolean isKnown = false;
    private long denominator = 0;
    private long numerator = 0;
    private boolean statusRequestCompleted = false;

    GearmanJobStatusImpl() {
    }

    public long getDenominator() {
        return denominator;
    }

    public long getNumerator() {
        return numerator;
    }

    public boolean isKnown() {
        return isKnown;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void handleEvent(GearmanPacket repsonse) throws GearmanException {
        if (!repsonse.getPacketType().equals(GearmanPacketType.STATUS_RES)) {
            throw new GearmanException("Dont know how to handle response of " +
                    "type " + repsonse);
        }

        isKnown = (repsonse.getDataComponentValue(
                GearmanPacket.DataComponentName.KNOWN_STATUS))[0] == '0' ?
                    false : true;
        isRunning = (repsonse.getDataComponentValue(
                GearmanPacket.DataComponentName.RUNNING_STATUS))[0] == '0' ?
                    false : true;
        numerator = Long.parseLong(ByteUtils.fromUTF8Bytes(
                repsonse.getDataComponentValue(
                GearmanPacket.DataComponentName.NUMERATOR)));
        denominator = Long.parseLong(ByteUtils.fromUTF8Bytes(
                repsonse.getDataComponentValue(
                GearmanPacket.DataComponentName.DENOMINATOR)));
        statusRequestCompleted = true;
    }

    public boolean isDone() {
        return statusRequestCompleted;
    }

    @Override
    public String toString() {
        return DESCRIPTION;
    }
}
