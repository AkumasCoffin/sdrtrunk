/*
 * *****************************************************************************
 * Copyright (C) 2014-2025 Dennis Sheirer
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 * ****************************************************************************
 */
package io.github.dsheirer.control;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.dsheirer.source.tuner.Tuner;
import io.github.dsheirer.source.tuner.manager.DiscoveredTuner;
import io.github.dsheirer.source.tuner.ui.DiscoveredTunerModel;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.java_websocket.WebSocket;
import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loopback-only WebSocket server that streams spectrum frames to authenticated clients.  Bound to control-port + 1.
 *
 * Client -> server text messages:
 *   {"t":"spectrumStart","tunerId":"&lt;id&gt;","fps":10,"bins":512}
 *   {"t":"spectrumStop","tunerId":"&lt;id&gt;"}
 */
public class ControlWebSocketServer extends WebSocketServer
{
    private static final Logger mLog = LoggerFactory.getLogger(ControlWebSocketServer.class);

    private final ControlServer mControlServer;
    private final String mToken;

    //Per-connection map of tunerId -> active streamer
    private final Map<WebSocket,Map<String,SpectrumStreamer>> mStreamers = new ConcurrentHashMap<>();

    /**
     * Constructs the spectrum WebSocket server.
     * @param address to bind (loopback, control-port + 1).
     * @param controlServer owning control server for tuner and mapper access.
     * @param token bearer token required for handshake authentication.
     */
    public ControlWebSocketServer(InetSocketAddress address, ControlServer controlServer, String token)
    {
        super(address);
        mControlServer = controlServer;
        mToken = token;
        setReuseAddr(true);
    }

    @Override
    public void onStart()
    {
        mLog.info("Control spectrum WebSocket server started on " + getAddress());
    }

    @Override
    public void onOpen(WebSocket conn, ClientHandshake handshake)
    {
        if(!isAuthorized(handshake, conn))
        {
            mLog.warn("Rejecting unauthorized spectrum WebSocket connection");
            conn.close();
            return;
        }

        mStreamers.put(conn, new ConcurrentHashMap<>());
    }

    /**
     * Validates the bearer token from the Authorization header or a ?token= query parameter.
     */
    private boolean isAuthorized(ClientHandshake handshake, WebSocket conn)
    {
        String provided = null;

        String authHeader = handshake.getFieldValue("Authorization");

        if(authHeader != null && authHeader.startsWith("Bearer "))
        {
            provided = authHeader.substring(7);
        }

        if(provided == null)
        {
            //Fall back to ?token= query parameter in the resource descriptor.
            String resource = handshake.getResourceDescriptor();

            if(resource != null)
            {
                int idx = resource.indexOf("token=");

                if(idx >= 0)
                {
                    provided = resource.substring(idx + "token=".length());
                    int amp = provided.indexOf('&');

                    if(amp >= 0)
                    {
                        provided = provided.substring(0, amp);
                    }
                }
            }
        }

        byte[] a = (provided == null ? "" : provided).getBytes(StandardCharsets.UTF_8);
        byte[] b = (mToken == null ? "" : mToken).getBytes(StandardCharsets.UTF_8);

        return MessageDigest.isEqual(a, b);
    }

    @Override
    public void onMessage(WebSocket conn, String message)
    {
        Map<String,SpectrumStreamer> connStreamers = mStreamers.get(conn);

        if(connStreamers == null)
        {
            //Not authorized / not tracked
            return;
        }

        try
        {
            JsonNode node = mControlServer.getMapper().readTree(message);
            String type = node.has("t") ? node.get("t").asText() : null;

            if(type == null)
            {
                return;
            }

            switch(type)
            {
                case "spectrumStart":
                    handleSpectrumStart(conn, connStreamers, node);
                    break;
                case "spectrumStop":
                    handleSpectrumStop(connStreamers, node);
                    break;
                default:
                    //ignore unknown message types
                    break;
            }
        }
        catch(Exception e)
        {
            mLog.warn("Error handling spectrum WebSocket message", e);
        }
    }

    private void handleSpectrumStart(WebSocket conn, Map<String,SpectrumStreamer> connStreamers, JsonNode node)
    {
        String tunerId = node.has("tunerId") ? node.get("tunerId").asText() : null;

        if(tunerId == null)
        {
            return;
        }

        int fps = node.has("fps") ? node.get("fps").asInt(10) : 10;
        int bins = node.has("bins") ? node.get("bins").asInt(512) : 512;

        DiscoveredTunerModel model = mControlServer.getTunerManager().getDiscoveredTunerModel();
        DiscoveredTuner discoveredTuner = model.getDiscoveredTuner(tunerId);

        if(discoveredTuner == null || !discoveredTuner.hasTuner())
        {
            mLog.warn("Spectrum start requested for unavailable tuner [" + tunerId + "]");
            return;
        }

        //Replace any existing stream for this tuner on this connection.
        SpectrumStreamer existing = connStreamers.remove(tunerId);

        if(existing != null)
        {
            existing.stop();
        }

        Tuner tuner = discoveredTuner.getTuner();
        int index = model.getAvailableTuners().indexOf(discoveredTuner);
        SpectrumStreamer streamer = new SpectrumStreamer(tuner, Math.max(index, 0), conn);
        streamer.start(bins, fps);
        connStreamers.put(tunerId, streamer);
    }

    private void handleSpectrumStop(Map<String,SpectrumStreamer> connStreamers, JsonNode node)
    {
        String tunerId = node.has("tunerId") ? node.get("tunerId").asText() : null;

        if(tunerId == null)
        {
            return;
        }

        SpectrumStreamer streamer = connStreamers.remove(tunerId);

        if(streamer != null)
        {
            streamer.stop();
        }
    }

    @Override
    public void onClose(WebSocket conn, int code, String reason, boolean remote)
    {
        detach(conn);
    }

    @Override
    public void onError(WebSocket conn, Exception ex)
    {
        mLog.warn("Spectrum WebSocket error", ex);

        if(conn != null)
        {
            detach(conn);
        }
    }

    /**
     * Stops and removes all streamers for the given connection.
     */
    private void detach(WebSocket conn)
    {
        Map<String,SpectrumStreamer> connStreamers = mStreamers.remove(conn);

        if(connStreamers != null)
        {
            for(SpectrumStreamer streamer : connStreamers.values())
            {
                streamer.stop();
            }

            connStreamers.clear();
        }
    }

    /**
     * Stops and removes all streamers across all connections.
     */
    public void detachAll()
    {
        for(Map<String,SpectrumStreamer> connStreamers : mStreamers.values())
        {
            for(SpectrumStreamer streamer : connStreamers.values())
            {
                streamer.stop();
            }

            connStreamers.clear();
        }

        mStreamers.clear();
    }
}
