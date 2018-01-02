/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.voltdb.mapred.inputformat;

import java.io.IOException;
import java.net.UnknownHostException;

import org.apache.hadoop.mapreduce.JobContext;
import org.voltdb.VoltTable;
import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;
import org.voltdb.client.ProcCallException;

public class VoltClientShim {
	Client client = null;
	//JobContext ctx;
	String hostname;

	VoltClientShim(JobContext ctx) {
		this.hostname = "localhost";
	}

	VoltClientShim(String hostname) {
		this.hostname = hostname;
	}

	private void connectIfNeeded() {
		if (client != null) {
			// need to check for dead connections, but not now
			return;
		}

		//Configuration jobConf = ctx.getConfiguration();

		ClientConfig config = new ClientConfig();
		client = ClientFactory.createClient(config);
		try {
			client.createConnection(hostname);
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	VoltTable query(String sql, Object... params) {
		connectIfNeeded();

		Object[] fullParams = new Object[params.length + 1];
		fullParams[0] = sql;
		for (int i = 0; i < params.length; i++) {
			fullParams[i + 1] = params[i];
		}

		try {
			ClientResponse response = client.callProcedure("@AdHoc", fullParams);
			return response.getResults()[0];
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
			return null;
		}
	}

	VoltTable queryPartition(String sql, long partitionKey) {
		connectIfNeeded();

		try {
			ClientResponse response = client.callProcedure("@AdHocSpForTest", sql, partitionKey);
			return response.getResults()[0];
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
			return null;
		}
	}

	long[] getPartitionKeys() {
		connectIfNeeded();

		try {
			ClientResponse response = client.callProcedure("@GetPartitionKeys", "INTEGER");
			VoltTable t = response.getResults()[0];
			long[] retval = new long[t.getRowCount()];
			for (int i = 0; i < t.getRowCount(); i++) {
				retval[i] = t.fetchRow(i).getLong(1);
			}
			return retval;
		} catch (IOException | ProcCallException e) {
			e.printStackTrace();
			return null;
		}

	}

	public void close() {
		if (client != null) {
			try {
				client.close();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
}
