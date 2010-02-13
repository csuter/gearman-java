/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.functions;

import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.gearman.worker.AbstractGearmanFunction;

public class LongRunningFunction extends AbstractGearmanFunction {

    boolean keepRunning = true;

    public GearmanJobResult executeFunction() {
        while (keepRunning) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
        }
        return new GearmanJobResultImpl(jobHandle, true, new byte[0],
                new byte[0], new byte[0], 0, 0);
    }

    public void reset() {
        keepRunning = true;
    }

    public void isDone() {
        keepRunning = false;
    }

    public boolean isRunning() {
        return keepRunning;
    }
}