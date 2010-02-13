/*
 * Copyright (C) 2010 by Eric Lambert <eric.d.lambert@gmail.com>
 * Use and distribution licensed under the BSD license.  See
 * the COPYING file in the parent directory for full text.
 */
package org.gearman.tests.functions;

import org.gearman.client.GearmanJobResult;
import org.gearman.client.GearmanJobResultImpl;
import org.gearman.util.ByteUtils;
import org.gearman.worker.AbstractGearmanFunction;

public class ReverseFunction extends AbstractGearmanFunction {

    @Override
    public GearmanJobResult executeFunction() {
        StringBuffer sb = null;
        byte[] results = null;
        if (data instanceof byte[]) {
            sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte[]) data));
        } else {
            sb = new StringBuffer(data.toString());
        }
        results = sb.reverse().toString().getBytes();
        return new GearmanJobResultImpl(jobHandle, true, results,
                new byte[0], new byte[0], 0, 0);
    }
}
