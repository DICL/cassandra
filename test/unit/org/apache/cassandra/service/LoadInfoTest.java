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

import javax.management.InstanceNotFoundException;
import javax.management.MalformedObjectNameException;
import javax.management.ReflectionException;

import org.junit.Test;

import org.hyperic.sigar.SigarException;

public class LoadInfoTest
{
    @Test
    public void testLoadInfo() throws MalformedObjectNameException, InstanceNotFoundException, ReflectionException, SigarException
    {
        SystemLoadInfoService loadInfoService = new SystemLoadInfoService();
        int cpuLoad = loadInfoService.getCPUUtilization();
        int memLoad = loadInfoService.getMemoryUtilization();
        int diskLoad = loadInfoService.getDiskUtilization();
        System.out.printf("CPU load: %d; Memory load: %d; Disk space %d;", cpuLoad, memLoad, diskLoad);
    }
}
