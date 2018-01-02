/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
 * OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
 * OTHER DEALINGS IN THE SOFTWARE.
 */

package events;

import java.net.UnknownHostException;

import org.voltdb.SQLStmt;
import org.voltdb.VoltProcedure;
import org.voltdb.VoltTable;
import org.voltdb.types.TimestampType;

public class NewEvent extends VoltProcedure {
    final SQLStmt getCluster = new SQLStmt(
            "SELECT id, POWER(? - src, 2) + POWER(? - dest, 2) + POWER(? - referral, 2) + POWER(? - agent, 2) AS score "
            + "FROM clusters GROUP BY id ORDER BY score LIMIT 1;");

    final SQLStmt getUrlId = new SQLStmt(
            "SELECT id FROM dests WHERE url = ?;"
    );

    final SQLStmt getAgentId = new SQLStmt(
            "SELECT id FROM agents WHERE name = ?;"
    );

    final SQLStmt insertEvent = new SQLStmt(
            "INSERT INTO events (src, dest, method, ts, key, size, referral, agent, cluster) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);");

    public long run(int src, String dest, String method, TimestampType ts, long key, long size, String referral, String agent)
            throws UnknownHostException
    {
        voltQueueSQL(getUrlId, dest);
        voltQueueSQL(getUrlId, referral);
        voltQueueSQL(getAgentId, agent);
        final VoltTable[] batchResult = voltExecuteSQL();

        final int destId = (int) batchResult[0].asScalarLong();
        final int referralId = (int) batchResult[1].asScalarLong();
        final int agentId = (int) batchResult[2].asScalarLong();

        voltQueueSQL(getCluster, EXPECT_ZERO_OR_ONE_ROW, src, destId, referralId, agentId);
        final VoltTable[] secondResult = voltExecuteSQL();
        final VoltTable clusterResult = secondResult[0];
        Integer cluster = null;
        if (clusterResult.advanceRow()) {
            cluster = (int) clusterResult.getLong("id");
        }

        voltQueueSQL(insertEvent, EXPECT_SCALAR_LONG, src, destId, method, ts, key, size, referralId, agentId, cluster);
        voltExecuteSQL(true);

        return cluster == null ? -1 : cluster;
    }
}
