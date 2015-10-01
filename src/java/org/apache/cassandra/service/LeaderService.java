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

import java.net.InetAddress;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.cassandra.config.DatabaseDescriptor;

public class LeaderService
{
    private static final Random random = new Random();
    private static int points = 0;
    private static boolean isLeader;
    private static InetAddress leaderAddress;
    private static PriorityQueue<EndpointInfo> leadershipQueue;
    private static ConcurrentHashMap<InetAddress, Integer> leadershipMap;
    private static ConcurrentHashMap<InetAddress, LoadState> loadStateMap;
    // public static final LeaderService instance = new LeaderService();


    private static class LoadState
    {
        private int cpu;
        private int memory;
        private int disk;

        public LoadState(int cpu, int memory, int disk)
        {
            this.cpu = cpu;
            this.disk = disk;
            this.memory = memory;
        }
    }

    private static class EndpointInfo implements Comparator<EndpointInfo>
    {
        private InetAddress address;
        private int points;

        public EndpointInfo(InetAddress address, int points)
        {
            this.address = address;
            this.points = points;
        }

        @Override
        public int compare(EndpointInfo x, EndpointInfo y)
        {
            if (x.points < y.points)
                return -1;
            if (x.points > y.points)
                return 1;
            byte[] xAddress = x.address.getAddress();
            byte[] yAddress = y.address.getAddress();
            for (int i = 0; i < xAddress.length; i++)
            {
                int xByte = (int) xAddress[i] & 0xFF;
                int yByte = (int) yAddress[i] & 0xFF;
                if (xByte == yByte)
                    continue;
                if (xByte < yByte)
                    return -1;
                else
                    return 1;
            }
            return 0;
        }
    }

    public LeaderService()
    {
        points = random.nextInt() % Integer.MAX_VALUE + 1;
        isLeader = false;
        leaderAddress = DatabaseDescriptor.getListenAddress();
        leadershipQueue = new PriorityQueue<>();
        leadershipMap = new ConcurrentHashMap<>();
        loadStateMap = new ConcurrentHashMap<>();
    }

    /* Removes the address from the list when endpoint went DOWN */
    public static void removeFromLeaderList(InetAddress address)
    {
        leadershipQueue.remove(new EndpointInfo(address, leadershipMap.get(address)));
        leadershipMap.remove(address);
        if (address == leaderAddress)
            leaderAddress = DatabaseDescriptor.getListenAddress();
    }

    /*
     *  Insures that no newly added nodes will have equal or less points than current leader,
     *  setting it's leadership points to 0 */
    public static void setNextLeader()
    {
        try
        {
            Thread.sleep(60*1000);
        } catch (InterruptedException e)
        {
            Thread.currentThread().interrupt();
        }

        EndpointInfo newLeader = leadershipQueue.poll();
        newLeader.points = 0;
        leadershipMap.replace(newLeader.address, newLeader.points);
        leadershipQueue.add(newLeader);
        setLeaderAddress(newLeader.address);
    }

    public static void putToLeaderList(InetAddress address, int points)
    {
        if (leadershipMap.containsKey(address)) return;
        leadershipMap.put(address, points);
        leadershipQueue.add(new EndpointInfo(address, points));
    }


    public static int getPoints()
    {
        return points;
    }

    /* Makes this node a leader */
    public static void setLeadership()
    {
        isLeader = true;
        points = 0;
    }

    /* Assigns an address as an leader-address to send it system load information */
    public static void setLeaderAddress(InetAddress address)
    {
        leaderAddress = address;
        if (leaderAddress == DatabaseDescriptor.getListenAddress())
            setLeadership();
    }

    public static InetAddress getLeaderAddress()
    {
        return leaderAddress;
    }

    public static boolean isLeader()
    {
        return isLeader;
    }

    public static boolean hasLeader()
    {
        return isLeader ? true : (leaderAddress != DatabaseDescriptor.getListenAddress() ? true : false);
    }

    public static void refreshEndpointLoadState(InetAddress address, int cpu, int memory, int disk)
    {
        LoadState state = new LoadState(cpu, memory, disk);
        if (loadStateMap.containsKey(address))
            loadStateMap.replace(address, state);
        else
            loadStateMap.put(address, state);
    }

}
