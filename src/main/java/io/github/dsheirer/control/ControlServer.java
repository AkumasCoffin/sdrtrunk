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
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.dsheirer.channel.metadata.ChannelMetadata;
import io.github.dsheirer.channel.metadata.ChannelMetadataModel;
import io.github.dsheirer.controller.channel.Channel;
import io.github.dsheirer.controller.channel.ChannelException;
import io.github.dsheirer.controller.channel.ChannelModel;
import io.github.dsheirer.controller.channel.ChannelProcessingManager;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.monitor.DiagnosticMonitor;
import io.github.dsheirer.playlist.PlaylistManager;
import io.github.dsheirer.properties.SystemProperties;
import io.github.dsheirer.source.SourceException;
import io.github.dsheirer.source.tuner.Tuner;
import io.github.dsheirer.source.tuner.TunerController;
import io.github.dsheirer.source.tuner.manager.DiscoveredTuner;
import io.github.dsheirer.source.tuner.manager.TunerManager;
import io.github.dsheirer.source.tuner.ui.DiscoveredTunerModel;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Headless control server exposing a loopback-only REST API (control-port) plus a spectrum WebSocket (control-port + 1)
 * for remote monitoring and control of a headless SDR-Trunk instance.  All REST requests require a
 * {@code Authorization: Bearer &lt;token&gt;} header validated with a constant-time compare.
 */
public class ControlServer
{
    private static final Logger mLog = LoggerFactory.getLogger(ControlServer.class);
    private static final String CONTENT_TYPE_JSON = "application/json";

    private final TunerManager mTunerManager;
    private final PlaylistManager mPlaylistManager;
    private final DiagnosticMonitor mDiagnosticMonitor;
    private final boolean mHeadless;
    private final int mPort;
    private final String mToken;
    private final ObjectMapper mMapper = new ObjectMapper();

    private HttpServer mHttpServer;
    private ControlWebSocketServer mWsServer;
    private ExecutorService mExecutor;
    private EventBuffer mEventBuffer;
    private long mStartTime;

    /**
     * Constructs the control server.
     * @param tunerManager for tuner discovery and control.
     * @param playlistManager for channel discovery, control and playlist reload.
     * @param diagnosticMonitor diagnostic monitor (retained for future use).
     * @param headless whether the application is running headless.
     * @param port REST port (WebSocket binds to port + 1).
     * @param token bearer token from env SDRTRUNK_CONTROL_TOKEN.
     */
    public ControlServer(TunerManager tunerManager, PlaylistManager playlistManager, DiagnosticMonitor diagnosticMonitor,
                         boolean headless, int port, String token)
    {
        mTunerManager = tunerManager;
        mPlaylistManager = playlistManager;
        mDiagnosticMonitor = diagnosticMonitor;
        mHeadless = headless;
        mPort = port;
        mToken = token;
    }

    /**
     * Object mapper shared with the WebSocket server.
     */
    public ObjectMapper getMapper()
    {
        return mMapper;
    }

    /**
     * Tuner manager for WebSocket spectrum tuner resolution.
     */
    public TunerManager getTunerManager()
    {
        return mTunerManager;
    }

    /**
     * Starts the REST server (loopback) and the spectrum WebSocket server (loopback, port + 1).
     */
    public void start() throws IOException
    {
        if(mToken == null || mToken.isEmpty())
        {
            mLog.warn("Control server starting WITHOUT a token (env SDRTRUNK_CONTROL_TOKEN not set) - all requests " +
                    "will require an empty bearer token");
        }

        mStartTime = System.currentTimeMillis();

        mEventBuffer = new EventBuffer();
        mPlaylistManager.getChannelProcessingManager().addDecodeEventListener(mEventBuffer);

        mExecutor = Executors.newFixedThreadPool(4);

        mHttpServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), mPort), 0);
        mHttpServer.setExecutor(mExecutor);
        mHttpServer.createContext("/status", this::handleStatus);
        mHttpServer.createContext("/tuners", this::handleTuners);
        mHttpServer.createContext("/channels", this::handleChannels);
        mHttpServer.createContext("/events", this::handleEvents);
        mHttpServer.createContext("/playlist", this::handlePlaylist);
        mHttpServer.start();

        mWsServer = new ControlWebSocketServer(new InetSocketAddress(InetAddress.getLoopbackAddress(), mPort + 1),
                this, mToken);
        mWsServer.start();

        mLog.info("Control server started - REST on 127.0.0.1:" + mPort + ", spectrum WS on 127.0.0.1:" + (mPort + 1));
    }

    /**
     * Stops the REST + WebSocket servers and detaches all spectrum streamers.
     */
    public void stop()
    {
        mLog.info("Stopping control server ...");

        if(mWsServer != null)
        {
            try
            {
                mWsServer.detachAll();
                mWsServer.stop(1000);
            }
            catch(Exception e)
            {
                mLog.warn("Error stopping spectrum WebSocket server", e);
            }

            mWsServer = null;
        }

        if(mHttpServer != null)
        {
            mHttpServer.stop(0);
            mHttpServer = null;
        }

        if(mEventBuffer != null)
        {
            mPlaylistManager.getChannelProcessingManager().removeDecodeEventListener(mEventBuffer);
            mEventBuffer = null;
        }

        if(mExecutor != null)
        {
            mExecutor.shutdownNow();
            mExecutor = null;
        }
    }

    //---------------------------------------------------------------------------------------------------------------
    // REST handlers
    //---------------------------------------------------------------------------------------------------------------

    private void handleStatus(HttpExchange exchange)
    {
        try
        {
            if(!authorize(exchange))
            {
                return;
            }

            ChannelProcessingManager cpm = mPlaylistManager.getChannelProcessingManager();
            ChannelModel cm = mPlaylistManager.getChannelModel();

            int tunerCount = mTunerManager.getDiscoveredTunerModel().getAvailableTuners().size();
            List<Channel> channels = cm.getChannels();
            int channelCount = channels.size();
            int processing = 0;

            for(Channel channel : channels)
            {
                if(channel.isProcessing())
                {
                    processing++;
                }
            }

            double load = ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage();

            if(load < 0)
            {
                load = -1;
            }

            Runtime runtime = Runtime.getRuntime();
            long usedMB = (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
            long maxMB = runtime.maxMemory() / (1024 * 1024);

            Map<String,Object> body = new LinkedHashMap<>();
            body.put("version", SystemProperties.getInstance().getApplicationName());
            body.put("uptimeMs", System.currentTimeMillis() - mStartTime);
            body.put("headless", mHeadless);
            body.put("tuners", tunerCount);
            body.put("channels", channelCount);
            body.put("processing", processing);
            body.put("cpuLoad", load);
            body.put("cores", runtime.availableProcessors());
            body.put("memUsedMB", usedMB);
            body.put("memMaxMB", maxMB);

            sendJson(exchange, 200, body);
        }
        catch(Exception e)
        {
            sendError(exchange, e);
        }
        finally
        {
            exchange.close();
        }
    }

    private void handleTuners(HttpExchange exchange)
    {
        try
        {
            if(!authorize(exchange))
            {
                return;
            }

            String path = exchange.getRequestURI().getPath();

            if(path.equals("/tuners") || path.equals("/tuners/"))
            {
                sendJson(exchange, 200, buildTunerList());
                return;
            }

            //Expect /tuners/{id}/{action}
            String[] parts = path.split("/");

            if(parts.length >= 4)
            {
                String id = URLDecoder.decode(parts[2], StandardCharsets.UTF_8);
                String action = parts[3];
                handleTunerControl(exchange, id, action);
                return;
            }

            sendJson(exchange, 404, error("not found"));
        }
        catch(Exception e)
        {
            sendError(exchange, e);
        }
        finally
        {
            exchange.close();
        }
    }

    private Map<String,Object> buildTunerList()
    {
        List<DiscoveredTuner> tuners = mTunerManager.getDiscoveredTunerModel().getAvailableTuners();
        List<Map<String,Object>> list = new ArrayList<>();

        for(int index = 0; index < tuners.size(); index++)
        {
            DiscoveredTuner dt = tuners.get(index);
            Map<String,Object> entry = new LinkedHashMap<>();
            entry.put("index", index);
            entry.put("id", dt.getId());
            entry.put("tunerClass", String.valueOf(dt.getTunerClass()));
            entry.put("status", String.valueOf(dt.getTunerStatus()));
            entry.put("enabled", dt.isEnabled());
            entry.put("available", dt.isAvailable());

            if(dt.hasTuner())
            {
                Tuner t = dt.getTuner();
                TunerController c = t.getTunerController();
                entry.put("name", t.getPreferredName());
                entry.put("type", t.getTunerType() != null ? t.getTunerType().getLabel() : null);
                entry.put("frequency", c.getFrequency());
                entry.put("sampleRate", c.getSampleRate());
                entry.put("ppm", c.getFrequencyCorrection());
                entry.put("measuredPpmError", c.getPPMFrequencyError());
            }
            else
            {
                entry.put("name", null);
                entry.put("type", null);
                entry.put("frequency", 0L);
                entry.put("sampleRate", 0d);
                entry.put("ppm", 0d);
                entry.put("measuredPpmError", 0d);
            }

            entry.put("error", dt.hasErrorMessage() ? dt.getErrorMessage() : null);
            list.add(entry);
        }

        Map<String,Object> body = new LinkedHashMap<>();
        body.put("tuners", list);
        return body;
    }

    private void handleTunerControl(HttpExchange exchange, String id, String action) throws IOException
    {
        DiscoveredTuner dt = mTunerManager.getDiscoveredTunerModel().getDiscoveredTuner(id);

        if(dt == null || !dt.hasTuner())
        {
            sendJson(exchange, 200, error("tuner not available"));
            return;
        }

        TunerController c = dt.getTuner().getTunerController();
        JsonNode body = readBody(exchange);

        switch(action)
        {
            case "frequency":
            {
                long frequency = body.has("frequency") ? body.get("frequency").asLong() : 0L;

                try
                {
                    c.setFrequency(frequency);
                    Map<String,Object> ok = new LinkedHashMap<>();
                    ok.put("ok", true);
                    ok.put("frequency", frequency);
                    sendJson(exchange, 200, ok);
                }
                catch(SourceException se)
                {
                    sendJson(exchange, 200, error(se.getMessage()));
                }
                break;
            }
            case "ppm":
            {
                double ppm = body.has("ppm") ? body.get("ppm").asDouble() : 0d;

                try
                {
                    c.setFrequencyCorrection(ppm);
                    Map<String,Object> ok = new LinkedHashMap<>();
                    ok.put("ok", true);
                    ok.put("ppm", ppm);
                    sendJson(exchange, 200, ok);
                }
                catch(SourceException se)
                {
                    sendJson(exchange, 200, error(se.getMessage()));
                }
                break;
            }
            case "gain":
            {
                //TODO: gain is device-typed with no common interface - dispatch per TunerController subtype in a
                //      future iteration.  For now report as unsupported.
                sendJson(exchange, 200, error("gain control not supported for this tuner type"));
                break;
            }
            default:
                sendJson(exchange, 404, error("not found"));
                break;
        }
    }

    private void handleChannels(HttpExchange exchange)
    {
        try
        {
            if(!authorize(exchange))
            {
                return;
            }

            String path = exchange.getRequestURI().getPath();

            if(path.equals("/channels") || path.equals("/channels/"))
            {
                sendJson(exchange, 200, buildChannelList());
                return;
            }

            //Expect /channels/{id}/{action}
            String[] parts = path.split("/");

            if(parts.length >= 4)
            {
                int id;

                try
                {
                    id = Integer.parseInt(parts[2]);
                }
                catch(NumberFormatException nfe)
                {
                    sendJson(exchange, 200, error("invalid channel id"));
                    return;
                }

                String action = parts[3];
                handleChannelControl(exchange, id, action);
                return;
            }

            sendJson(exchange, 404, error("not found"));
        }
        catch(Exception e)
        {
            sendError(exchange, e);
        }
        finally
        {
            exchange.close();
        }
    }

    private Map<String,Object> buildChannelList()
    {
        ChannelProcessingManager cpm = mPlaylistManager.getChannelProcessingManager();
        ChannelModel cm = mPlaylistManager.getChannelModel();
        ChannelMetadataModel mm = cpm.getChannelMetadataModel();

        //Build a channel -> metadata lookup from the live metadata model.
        Map<Channel,ChannelMetadata> metaByChannel = new HashMap<>();
        int rows = mm.getRowCount();

        for(int r = 0; r < rows; r++)
        {
            try
            {
                ChannelMetadata meta = mm.getChannelMetadata(r);

                if(meta != null)
                {
                    Channel ch = mm.getChannelFromMetadata(meta);

                    if(ch != null)
                    {
                        metaByChannel.putIfAbsent(ch, meta);
                    }
                }
            }
            catch(Exception e)
            {
                //Metadata list mutates on the EDT - ignore transient index issues.
            }
        }

        List<Map<String,Object>> list = new ArrayList<>();

        for(Channel channel : cm.getChannels())
        {
            Map<String,Object> entry = new LinkedHashMap<>();
            entry.put("id", channel.getChannelID());
            entry.put("name", channel.getName());
            entry.put("system", channel.getSystem());
            entry.put("site", channel.getSite());
            entry.put("type", String.valueOf(channel.getChannelType()));

            boolean processing = channel.isProcessing();
            entry.put("processing", processing);

            if(processing)
            {
                ChannelMetadata meta = metaByChannel.get(channel);

                if(meta != null)
                {
                    Identifier state = meta.getChannelStateIdentifier();
                    entry.put("state", state != null ? state.toString() : "PROCESSING");

                    Identifier from = meta.getFromIdentifier();
                    entry.put("from", from != null ? from.toString() : null);

                    Identifier to = meta.getToIdentifier();
                    entry.put("to", to != null ? to.toString() : null);

                    entry.put("frequency", frequencyOf(meta));
                }
                else
                {
                    entry.put("state", "PROCESSING");
                    entry.put("from", null);
                    entry.put("to", null);
                    entry.put("frequency", null);
                }
            }
            else
            {
                entry.put("state", "STOPPED");
                entry.put("from", null);
                entry.put("to", null);
                entry.put("frequency", null);
            }

            list.add(entry);
        }

        Map<String,Object> body = new LinkedHashMap<>();
        body.put("channels", list);
        return body;
    }

    private Long frequencyOf(ChannelMetadata meta)
    {
        try
        {
            if(meta.getFrequencyConfigurationIdentifier() != null)
            {
                Object value = meta.getFrequencyConfigurationIdentifier().getValue();

                if(value instanceof Number number)
                {
                    return number.longValue();
                }
            }
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        return null;
    }

    private void handleChannelControl(HttpExchange exchange, int id, String action) throws IOException
    {
        ChannelProcessingManager cpm = mPlaylistManager.getChannelProcessingManager();
        ChannelModel cm = mPlaylistManager.getChannelModel();

        Channel target = null;

        for(Channel channel : cm.getChannels())
        {
            if(channel.getChannelID() == id)
            {
                target = channel;
                break;
            }
        }

        if(target == null)
        {
            sendJson(exchange, 200, error("channel not found"));
            return;
        }

        try
        {
            switch(action)
            {
                case "start":
                    cpm.start(target);
                    sendJson(exchange, 200, ok());
                    break;
                case "stop":
                    cpm.stop(target);
                    sendJson(exchange, 200, ok());
                    break;
                default:
                    sendJson(exchange, 404, error("not found"));
                    break;
            }
        }
        catch(ChannelException ce)
        {
            sendJson(exchange, 200, error(ce.getMessage()));
        }
    }

    private void handleEvents(HttpExchange exchange)
    {
        try
        {
            if(!authorize(exchange))
            {
                return;
            }

            int limit = 50;
            String query = exchange.getRequestURI().getQuery();

            if(query != null)
            {
                for(String pair : query.split("&"))
                {
                    if(pair.startsWith("limit="))
                    {
                        try
                        {
                            limit = Integer.parseInt(pair.substring("limit=".length()));
                        }
                        catch(NumberFormatException nfe)
                        {
                            //keep default
                        }
                    }
                }
            }

            Map<String,Object> body = new LinkedHashMap<>();
            body.put("events", mEventBuffer.getEvents(limit));
            sendJson(exchange, 200, body);
        }
        catch(Exception e)
        {
            sendError(exchange, e);
        }
        finally
        {
            exchange.close();
        }
    }

    private void handlePlaylist(HttpExchange exchange)
    {
        try
        {
            if(!authorize(exchange))
            {
                return;
            }

            String path = exchange.getRequestURI().getPath();

            if(path.equals("/playlist/reload"))
            {
                mPlaylistManager.init();

                ChannelProcessingManager cpm = mPlaylistManager.getChannelProcessingManager();
                ChannelModel cm = mPlaylistManager.getChannelModel();

                for(Channel channel : cm.getAutoStartChannels())
                {
                    try
                    {
                        cpm.start(channel);
                    }
                    catch(ChannelException ce)
                    {
                        mLog.warn("Auto-start failed for channel [" + channel.getName() + "] during playlist reload - " +
                                ce.getMessage());
                    }
                }

                sendJson(exchange, 200, ok());
                return;
            }

            sendJson(exchange, 404, error("not found"));
        }
        catch(Exception e)
        {
            sendError(exchange, e);
        }
        finally
        {
            exchange.close();
        }
    }

    //---------------------------------------------------------------------------------------------------------------
    // Helpers
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Validates the bearer token.  On failure, writes a 401 JSON response and returns false.
     */
    private boolean authorize(HttpExchange exchange) throws IOException
    {
        String header = exchange.getRequestHeaders().getFirst("Authorization");
        String provided = null;

        if(header != null && header.startsWith("Bearer "))
        {
            provided = header.substring(7);
        }

        byte[] a = (provided == null ? "" : provided).getBytes(StandardCharsets.UTF_8);
        byte[] b = (mToken == null ? "" : mToken).getBytes(StandardCharsets.UTF_8);

        if(MessageDigest.isEqual(a, b))
        {
            return true;
        }

        sendJson(exchange, 401, error("unauthorized"));
        exchange.close();
        return false;
    }

    private JsonNode readBody(HttpExchange exchange) throws IOException
    {
        try(InputStream is = exchange.getRequestBody())
        {
            byte[] bytes = is.readAllBytes();

            if(bytes.length == 0)
            {
                return mMapper.createObjectNode();
            }

            return mMapper.readTree(bytes);
        }
    }

    private Map<String,Object> ok()
    {
        Map<String,Object> map = new LinkedHashMap<>();
        map.put("ok", true);
        return map;
    }

    private Map<String,Object> error(String message)
    {
        Map<String,Object> map = new LinkedHashMap<>();
        map.put("ok", false);
        map.put("error", message == null ? "error" : message);
        return map;
    }

    private void sendError(HttpExchange exchange, Exception e)
    {
        mLog.warn("Control server request error", e);

        try
        {
            Map<String,Object> map = new LinkedHashMap<>();
            map.put("error", e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage());
            sendJson(exchange, 500, map);
        }
        catch(IOException ignore)
        {
            //nothing more we can do
        }
    }

    private void sendJson(HttpExchange exchange, int status, Object body) throws IOException
    {
        byte[] bytes = mMapper.writeValueAsBytes(body);
        exchange.getResponseHeaders().add("Content-Type", CONTENT_TYPE_JSON);
        exchange.sendResponseHeaders(status, bytes.length);

        try(OutputStream os = exchange.getResponseBody())
        {
            os.write(bytes);
        }
    }
}
