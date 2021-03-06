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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;

public class VoltInputSplit extends InputSplit implements Writable {

	long offset = 0, limit = 0, partitionKey = 0;
	JobContext ctx = null;

    VoltInputSplit(JobContext ctx, long partitionKey, long offset, long limit) {
    	this.offset = offset;
    	this.limit = limit;
    	this.partitionKey = partitionKey;
    	//this.ctx = ctx;
    }

    VoltInputSplit() {

    }

    @Override
    public long getLength() throws IOException {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException {
        // TODO Auto-generated method stub
        return new String[] { "none" };
    }

    @Override
    public void readFields(DataInput input) throws IOException {
        offset = input.readLong();
        limit = input.readLong();
        partitionKey = input.readLong();
    }

    @Override
    public void write(DataOutput output) throws IOException {
        output.writeLong(offset);
        output.writeLong(limit);
        output.writeLong(partitionKey);
    }

}
