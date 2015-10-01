package org.apache.cassandra.gms;

import java.io.IOException;
import java.net.InetAddress;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.LeaderService;

public class GossipLoadInfoVerbHandler implements IVerbHandler<GossipLoadInfo>
{
    private static final Logger logger = LoggerFactory.getLogger(GossipLoadInfoVerbHandler.class);

    public void doVerb(MessageIn<GossipLoadInfo> message, int id) throws IOException
    {
        InetAddress from = message.from;
        if (logger.isTraceEnabled())
            logger.trace("Received a GossipLoadInfoMessage from {} ", from);

        GossipLoadInfo gLoadInfo = message.payload;

        if (logger.isTraceEnabled())
            logger.trace("Gossip load info is: CPU {}, Memory {}, Disk {}",
                         gLoadInfo.cpu,
                         gLoadInfo.memory,
                         gLoadInfo.disk);

        /* Something went wrong */
        if (!LeaderService.isLeader())
            logger.error("Gossip to false leader from {} to {} ", from, DatabaseDescriptor.getListenAddress());
        else
        {
            LeaderService.instance.refreshEndpointLoadState(from, gLoadInfo.cpu, gLoadInfo.memory, gLoadInfo.disk);
        }

    }
}
