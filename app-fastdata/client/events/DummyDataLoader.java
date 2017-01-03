/* This file is part of VoltDB.
 * Copyright (C) 2008-2017 VoltDB Inc.
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

import org.voltdb.client.Client;
import org.voltdb.client.ClientConfig;
import org.voltdb.client.ClientFactory;
import org.voltdb.client.ClientResponse;

public class DummyDataLoader {

	public static void main(String[] args) throws Exception {
		ClientConfig config = new ClientConfig();
		Client client = ClientFactory.createClient(config);
		client.createConnection("localhost");

		for (int i = 0; i < 9461; i++) {

			int src = i;
			int dest = i;
			String method = "ABC";
			long ts = i;
			long key = i;
			long size = i;
			int referral = i;
			int agent = i;
			int cluster = i;

			ClientResponse response = client.callProcedure("EVENTS.insert", src, dest, method, ts, key, size, referral, agent, cluster);
			assert(response.getStatus() == ClientResponse.SUCCESS);
			assert(response.getResults().length == 1);
			assert(response.getResults()[0].asScalarLong() == 1);
		}

		client.close();

	}

}
