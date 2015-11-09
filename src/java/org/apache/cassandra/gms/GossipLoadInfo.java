/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.gms;

import java.io.DataInput;
import java.io.IOException;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.IVersionedSerializer;
import org.apache.cassandra.io.util.DataOutputPlus;

public class GossipLoadInfo
{
    public static final IVersionedSerializer<GossipLoadInfo> serializer = new GossipLoadInfoSerializer();

    final int cpu;
    final int memory;
    final int disk;

    public GossipLoadInfo(int cpu, int memory, int disk)
    {
        this.cpu = cpu;
        this.memory = memory;
        this.disk = disk;
    }
}

class GossipLoadInfoSerializer implements IVersionedSerializer<GossipLoadInfo>
{

    public void serialize(GossipLoadInfo gLoadInfo, DataOutputPlus out, int version) throws IOException
    {
        out.writeInt(gLoadInfo.cpu);
        out.writeInt(gLoadInfo.memory);
        out.writeInt(gLoadInfo.disk);
    }

    public GossipLoadInfo deserialize(DataInput in, int version) throws IOException
    {
        int cpu = in.readInt();
        int memory = in.readInt();
        int disk = in.readInt();
        return new GossipLoadInfo(cpu, memory, disk);
    }

    public long serializedSize(GossipLoadInfo gLoadInfo, int version)
    {
        long size = TypeSizes.NATIVE.sizeof(gLoadInfo.cpu);
        size += TypeSizes.NATIVE.sizeof(gLoadInfo.memory);
        size += TypeSizes.NATIVE.sizeof(gLoadInfo.disk);
        return size;
    }
}
