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

public class IncrementalReverseFunction extends AbstractGearmanFunction {

    public GearmanJobResult executeFunction() {
        StringBuffer sb = null;
        if (data instanceof byte[]) {
            sb = new StringBuffer(ByteUtils.fromUTF8Bytes((byte[]) data));
        } else {
            sb = new StringBuffer(data.toString());
        }
        sb = sb.reverse();

        for (int i = 0; i < sb.length(); i++) {
            sendData(sb.substring(i, i + 1).getBytes());
        }
        return new GearmanJobResultImpl(jobHandle, true, new byte[0],
                new byte[0], new byte[0], 0, 0);
    }
}
