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

package org.apache.cassandra.service;

import org.hyperic.sigar.CpuPerc;
import org.hyperic.sigar.FileSystemUsage;
import org.hyperic.sigar.Mem;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;

public class SystemLoadInfoService
{
    private static Sigar sigar;


    public SystemLoadInfoService()
    {
        sigar = new Sigar();
    }

    public static int getCPUUtilization() throws SigarException
    {
        CpuPerc cpuPerc = sigar.getCpuPerc();
        return (int) (cpuPerc.getCombined() * 100);
    }

    public static int getMemoryUtilization() throws SigarException
    {
        Mem mem = sigar.getMem();
        return (int) (mem.getUsedPercent());
    }

    public static int getDiskUtilization() throws SigarException
    {
        FileSystemUsage fileSystemUsage = sigar.getFileSystemUsage("C:");
        return (int) (fileSystemUsage.getUsePercent());
    }
}
