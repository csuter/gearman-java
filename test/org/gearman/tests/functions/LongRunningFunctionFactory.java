/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.functions;

import org.gearman.worker.GearmanFunction;
import org.gearman.worker.GearmanFunctionFactory;

public class LongRunningFunctionFactory implements GearmanFunctionFactory {

    LongRunningFunction lrf = null;

    public String getFunctionName() {
        return LongRunningFunction.class.getCanonicalName();
    }

    public synchronized GearmanFunction getFunction() {
        if (lrf == null) {
            lrf = new LongRunningFunction();
        }
        return lrf;
    }
}
