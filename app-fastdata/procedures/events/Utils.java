/* This file is part of VoltDB.
 * Copyright (C) 2008-2015 VoltDB Inc.
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

import java.net.InetAddress;
import java.net.UnknownHostException;

public class Utils {
    public static String itoip(int ip) throws UnknownHostException
    {
        return InetAddress.getByAddress(new byte[] {
                (byte)((ip >>> 24) & 0xff),
                (byte)((ip >>> 16) & 0xff),
                (byte)((ip >>>  8) & 0xff),
                (byte)((ip       ) & 0xff)
        }).getHostAddress();
    }

    public static int iptoi(String ip) throws UnknownHostException
    {
        final byte[] p = InetAddress.getByName(ip).getAddress();
        return (((p[0] << 24) & 0xff000000) |
                ((p[1] << 16) & 0x00ff0000) |
                ((p[2] <<  8) & 0x0000ff00) |
                ((p[3]      ) & 0x000000ff));
    }
}
