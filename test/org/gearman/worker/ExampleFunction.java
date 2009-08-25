/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package org.gearman.worker;

import org.gearman.client.GearmanIOEventListener;
import org.gearman.client.GearmanJobResult;
import org.gearman.common.GearmanPacket;


public class ExampleFunction implements GearmanFunction{
    private String name = this.getClass().getCanonicalName();
    Object data = null;

    public String getName() {
        return name;
    }

    public void setData(Object data) {
        this.data = data;
    }

    public GearmanJobResult call() throws Exception {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void setJobHandle(byte[] handle) throws IllegalArgumentException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public byte[] getJobHandle() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void registerEventListener(GearmanIOEventListener listener) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    public void fireEvent(GearmanPacket event) {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
