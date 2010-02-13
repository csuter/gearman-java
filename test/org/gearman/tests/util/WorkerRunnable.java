/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.util;

import org.gearman.worker.GearmanWorker;

public class WorkerRunnable implements Runnable {

    GearmanWorker myWorker = null;
    boolean isRunning = true;

    public WorkerRunnable(GearmanWorker w) {
        myWorker = w;
    }

    public void run() {
        while (isRunning) {
            myWorker.work();
        }
    }

    public void stop() {
        isRunning = false;
    }
}
