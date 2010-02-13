/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.functions;

import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.gearman.worker.AbstractGearmanFunction;

public class FailedFunction extends AbstractGearmanFunction {

    @Override
    public GearmanJobResult executeFunction() {
        return new GearmanJobResultImpl(jobHandle, false, new byte[0],
                new byte[0], new byte[0], 0, 0);
    }
}
