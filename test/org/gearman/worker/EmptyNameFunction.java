/*
 * Copyright (C) 2009 by Eric Lambert <Eric.Lambert@sun.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.worker;

import org.gearman.client.GearmanIOEventListener;
import org.gearman.common.GearmanPacket;

public class EmptyNameFunction implements GearmanFunction {

    public String getName() {
        return " ";
    }

    public void setData(Object data) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setJobHandle(byte[] handle) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public byte[] getJobHandle() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void registerEventListener(GearmanIOEventListener listener)
            throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void fireEvent(GearmanPacket event) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public GearmanPacket call() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
