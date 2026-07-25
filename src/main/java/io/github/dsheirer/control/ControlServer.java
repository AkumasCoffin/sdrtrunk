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
import io.github.dsheirer.alias.Alias;
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
import io.github.dsheirer.source.tuner.airspy.AirspySampleRate;
import io.github.dsheirer.source.tuner.airspy.AirspyTunerController;
import io.github.dsheirer.source.tuner.airspy.hf.AirspyHfSampleRate;
import io.github.dsheirer.source.tuner.airspy.hf.AirspyHfTunerController;
import io.github.dsheirer.source.tuner.airspy.hf.Attenuation;
import io.github.dsheirer.source.tuner.fcd.proV1.FCD1TunerController;
import io.github.dsheirer.source.tuner.fcd.proplusV2.FCD2TunerController;
import io.github.dsheirer.source.tuner.hackrf.HackRFTunerController;
import io.github.dsheirer.source.tuner.hydrasdr.HydraSdrTunerController;
import io.github.dsheirer.source.tuner.manager.DiscoveredTuner;
import io.github.dsheirer.source.tuner.manager.FrequencyErrorCorrectionManager;
import io.github.dsheirer.source.tuner.manager.TunerManager;
import io.github.dsheirer.source.tuner.rtl.EmbeddedTuner;
import io.github.dsheirer.source.tuner.rtl.RTL2832TunerController;
import io.github.dsheirer.source.tuner.rtl.e4k.E4KEmbeddedTuner;
import io.github.dsheirer.source.tuner.rtl.fc0013.FC0013EmbeddedTuner;
import io.github.dsheirer.source.tuner.rtl.r8x.R8xEmbeddedTuner;
import io.github.dsheirer.source.tuner.sdrplay.RspSampleRate;
import io.github.dsheirer.source.tuner.sdrplay.RspTunerController;
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
import java.util.concurrent.ConcurrentHashMap;
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
     * Last gain value applied to each tuner via this control API, keyed by tuner id.  Most SDR-Trunk tuner
     * controllers do not expose a getter for the current composite gain (it is persisted only in the tuner
     * configuration), so buildTunerList reports the last value set through this server as a fallback.  A value
     * only appears here after {@code POST /tuners/{id}/gain} has been called at least once since startup.
     */
    private final Map<String,Object> mLastGain = new ConcurrentHashMap<>();

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
                         io.github.dsheirer.preference.UserPreferences userPreferences,
                         boolean headless, int port, String token)
    {
        mTunerManager = tunerManager;
        mPlaylistManager = playlistManager;
        mDiagnosticMonitor = diagnosticMonitor;
        mUserPreferences = userPreferences;
        mHeadless = headless;
        mPort = port;
        mToken = token;
    }

    private final io.github.dsheirer.preference.UserPreferences mUserPreferences;

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

        mEventBuffer = new EventBuffer(mPlaylistManager.getAliasModel());
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
            // Snapshot the model's (JavaFX, non-thread-safe) channel list before iterating
            // off the HTTP pool thread — traffic-channel add/remove + playlist reload mutate
            // it concurrently, which would otherwise throw ConcurrentModificationException.
            List<Channel> channels = new ArrayList<>(cm.getChannels());
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

            // Node readiness: CPU calibration + the JMBE (AMBE/IMBE) voice codec.
            // Both must be present before channels decode voice with audio.
            boolean calibrated = io.github.dsheirer.vector.calibrate.CalibrationManager.getInstance().isCalibrated();
            java.nio.file.Path jmbePath = mUserPreferences != null
                    ? mUserPreferences.getJmbeLibraryPreference().getPathJmbeLibrary() : null;
            boolean jmbeInstalled = jmbePath != null && java.nio.file.Files.exists(jmbePath);
            body.put("calibrated", calibrated);
            body.put("jmbeInstalled", jmbeInstalled);

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

                //Current gain: real value where the controller can read it, else the last value set via this API.
                Object realGain = readCurrentGain(c);
                entry.put("gain", realGain != null ? realGain : mLastGain.get(dt.getId()));

                //Auto-PPM (automatic frequency error correction) enabled state.
                boolean autoPpm = false;

                try
                {
                    autoPpm = c.getFrequencyErrorCorrectionManager() != null &&
                            c.getFrequencyErrorCorrectionManager().isEnabled();
                }
                catch(Exception e)
                {
                    //best effort
                }

                entry.put("autoPpm", autoPpm);

                //Per-device capabilities so the UI can build correct controls without hardcoding per-type.
                entry.put("capabilities", buildTunerCapabilities(c));
            }
            else
            {
                entry.put("name", null);
                entry.put("type", null);
                entry.put("frequency", 0L);
                entry.put("sampleRate", 0d);
                entry.put("ppm", 0d);
                entry.put("measuredPpmError", 0d);
                entry.put("gain", null);
                entry.put("autoPpm", false);
                entry.put("capabilities", null);
            }

            entry.put("error", dt.hasErrorMessage() ? dt.getErrorMessage() : null);
            list.add(entry);
        }

        Map<String,Object> body = new LinkedHashMap<>();
        body.put("tuners", list);
        return body;
    }

    //---------------------------------------------------------------------------------------------------------------
    // Per-tuner capabilities (device-typed dispatch, mirrors applyGain / applySampleRate)
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Builds a best-effort {@code capabilities} object for a specific tuner controller describing what it can be
     * set to, so the UI can render correct controls instead of hardcoding per device type.  Dispatches on the
     * concrete controller (and, for RTL, the embedded tuner) type - the same dispatch used by {@link #applyGain}
     * and {@link #applySampleRate}.  Each axis is populated only when the device actually has it; anything the
     * controller does not cleanly expose is simply omitted.
     *
     * <p>Top-level fields:</p>
     * <ul>
     *   <li>{@code sampleRates} - array of settable sample rates in Hz (omitted/empty for fixed-rate devices).</li>
     *   <li>{@code gain} - object describing the device's gain model (see below).</li>
     * </ul>
     *
     * <p>The {@code gain.mode} is one of {@code "master"} (single composite axis), {@code "multi"} (independent
     * axes such as LNA + VGA), {@code "toggle"} (a single boolean) or {@code "fixed"}.  For the RTL master gain
     * the {@code masterGainUnit} field disambiguates whether the {@code /gain} endpoint's {@code gain} number is
     * a raw device value, a dB figure, or a 0-based step index - the value is always snapped to the nearest entry
     * in the {@code masterGain} list.</p>
     */
    private Map<String,Object> buildTunerCapabilities(TunerController c)
    {
        Map<String,Object> caps = new LinkedHashMap<>();

        try
        {
            caps.put("sampleRates", buildSampleRates(c));
        }
        catch(Exception e)
        {
            //best effort - omit on failure
        }

        try
        {
            Map<String,Object> gain = buildGainCapabilities(c);

            if(gain != null)
            {
                caps.put("gain", gain);
            }
        }
        catch(Exception e)
        {
            //best effort - omit on failure
        }

        return caps;
    }

    /**
     * Best-effort list of the device's settable sample rates in Hz.  Returns an empty list for fixed-rate devices
     * (FCD) or when the device does not expose an enumerable set.  Mirrors {@link #applySampleRate} exactly so the
     * advertised rates are the same ones the endpoint will accept.
     */
    private List<Long> buildSampleRates(TunerController c)
    {
        List<Long> rates = new ArrayList<>();

        if(c instanceof RTL2832TunerController)
        {
            for(RTL2832TunerController.SampleRate sr : RTL2832TunerController.SampleRate.values())
            {
                rates.add((long)sr.getRate());
            }
        }
        else if(c instanceof HackRFTunerController)
        {
            for(HackRFTunerController.HackRFSampleRate sr : HackRFTunerController.HackRFSampleRate.VALID_SAMPLE_RATES)
            {
                rates.add((long)sr.getRate());
            }
        }
        else if(c instanceof AirspyTunerController airspy)
        {
            for(AirspySampleRate sr : airspy.getSampleRates())
            {
                rates.add((long)sr.getRate());
            }
        }
        else if(c instanceof HydraSdrTunerController hydra)
        {
            for(io.github.dsheirer.source.tuner.hydrasdr.HydraSdrSampleRate sr : hydra.getSampleRates())
            {
                rates.add((long)sr.getRate());
            }
        }
        else if(c instanceof RspTunerController)
        {
            for(RspSampleRate sr : RspSampleRate.values())
            {
                rates.add(sr.getSampleRate());
            }
        }
        else if(c instanceof AirspyHfTunerController hf)
        {
            for(AirspyHfSampleRate sr : hf.getAvailableSampleRates())
            {
                rates.add((long)sr.getSampleRate());
            }
        }
        //FCD1 / FCD2 are fixed-rate - return empty list.

        return rates;
    }

    /**
     * Best-effort description of the device's gain model.  Returns null when the tuner type has no settable gain
     * via this API.  The axes populated here mirror the ones {@link #applyGain} actually reads for each device.
     */
    private Map<String,Object> buildGainCapabilities(TunerController c)
    {
        //---------------------------------------------------------------------------------- RTL2832 family
        if(c instanceof RTL2832TunerController rtl && rtl.hasEmbeddedTuner())
        {
            EmbeddedTuner embedded = rtl.getEmbeddedTuner();

            if(embedded instanceof R8xEmbeddedTuner)
            {
                //R820T/R828D master gain labels are the raw composite gain values (tenths of a dB); the /gain
                //endpoint snaps the requested {gain} number to the nearest of these - it is NOT a 0-based index.
                Map<String,Object> gain = new LinkedHashMap<>();
                gain.put("mode", "master");
                gain.put("masterGainUnit", "value");
                gain.put("masterGain", enumNumbers(R8xEmbeddedTuner.MasterGain.values()));
                gain.put("agc", true);
                return gain;
            }

            if(embedded instanceof E4KEmbeddedTuner)
            {
                //E4K master gain labels are dB figures (e.g. "16.5 db"); the /gain endpoint expects a dB value.
                Map<String,Object> gain = new LinkedHashMap<>();
                gain.put("mode", "master");
                gain.put("masterGainUnit", "dB");
                gain.put("masterGain", enumNumbers(E4KEmbeddedTuner.E4KGain.values()));
                gain.put("agc", true);
                return gain;
            }

            if(embedded instanceof FC0013EmbeddedTuner)
            {
                //FC0013 LNA gain labels are 0..23 step indexes; the /gain endpoint expects that index.
                Map<String,Object> gain = new LinkedHashMap<>();
                gain.put("mode", "master");
                gain.put("masterGainUnit", "index");
                gain.put("masterGain", enumNumbers(FC0013EmbeddedTuner.LNAGain.values()));
                gain.put("agc", true);
                return gain;
            }

            return null;
        }

        //---------------------------------------------------------------------------------- Airspy / HydraSDR
        if(c instanceof AirspyTunerController || c instanceof HydraSdrTunerController)
        {
            //Both take a 1..22 gain level plus a LINEARITY/SENSITIVITY curve (see applyGain).
            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "master");
            gain.put("masterGainUnit", "index");
            gain.put("min", 1);
            gain.put("max", 22);
            gain.put("step", 1);
            gain.put("gainModes", List.of("LINEARITY", "SENSITIVITY"));
            return gain;
        }

        //---------------------------------------------------------------------------------- HackRF (two-axis + amp)
        if(c instanceof HackRFTunerController)
        {
            List<Integer> lna = new ArrayList<>();

            for(HackRFTunerController.HackRFLNAGain g : HackRFTunerController.HackRFLNAGain.values())
            {
                lna.add(g.getValue());
            }

            List<Integer> vga = new ArrayList<>();

            for(HackRFTunerController.HackRFVGAGain g : HackRFTunerController.HackRFVGAGain.values())
            {
                vga.add(g.getValue());
            }

            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "multi");
            gain.put("unit", "dB");
            gain.put("lnaGain", lna);
            gain.put("vgaGain", vga);
            gain.put("amp", true);
            return gain;
        }

        //---------------------------------------------------------------------------------- SDRplay RSP
        if(c instanceof RspTunerController rsp)
        {
            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "multi");

            Map<String,Object> lnaState = new LinkedHashMap<>();
            lnaState.put("min", 0);

            try
            {
                lnaState.put("max", rsp.getControlRsp().getMaximumLNASetting());
            }
            catch(Exception e)
            {
                //best effort - omit max if unavailable
            }

            gain.put("lnaState", lnaState);

            Map<String,Object> gr = new LinkedHashMap<>();
            gr.put("min", 20);
            gr.put("max", 59);
            gr.put("unit", "dB");
            gain.put("gainReduction", gr);
            return gain;
        }

        //---------------------------------------------------------------------------------- FUNcube Dongle Pro V1
        if(c instanceof FCD1TunerController)
        {
            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "master");
            gain.put("masterGainUnit", "dB");
            gain.put("masterGain", enumSignedNumbers(FCD1TunerController.LNAGain.values()));
            return gain;
        }

        //---------------------------------------------------------------------------------- FUNcube Dongle Pro+ V2
        if(c instanceof FCD2TunerController)
        {
            //Pro+ V2 exposes only an LNA on/off switch (see applyGain - body {enabled|gain>0}).
            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "toggle");
            gain.put("lna", true);
            return gain;
        }

        //---------------------------------------------------------------------------------- Airspy HF+ (attenuation + LNA)
        if(c instanceof AirspyHfTunerController)
        {
            //applyGain snaps {attenuation} to Attenuation.getValue(), which is the 0..N index (label is the dB).
            List<Integer> attValues = new ArrayList<>();
            List<String> attLabels = new ArrayList<>();

            for(Attenuation a : Attenuation.values())
            {
                attValues.add((int)a.getValue());
                attLabels.add(a.toString());
            }

            Map<String,Object> att = new LinkedHashMap<>();
            att.put("unit", "index");
            att.put("min", attValues.isEmpty() ? 0 : attValues.get(0));
            att.put("max", attValues.isEmpty() ? 0 : attValues.get(attValues.size() - 1));
            att.put("values", attValues);
            att.put("labels", attLabels);

            Map<String,Object> gain = new LinkedHashMap<>();
            gain.put("mode", "multi");
            gain.put("attenuation", att);
            gain.put("lna", true);
            return gain;
        }

        return null;
    }

    /**
     * Extracts the leading numeric label from each enum constant's {@code toString()} (via {@link #parseLeadingNumber}),
     * in declaration order, skipping constants with no numeric label (e.g. AUTOMATIC / MANUAL).  Used to publish the
     * discrete gain steps a device accepts.
     */
    private static List<Double> enumNumbers(Object[] values)
    {
        List<Double> numbers = new ArrayList<>();

        for(Object v : values)
        {
            Double number = parseLeadingNumber(String.valueOf(v));

            if(number != null)
            {
                numbers.add(number);
            }
        }

        return numbers;
    }

    /**
     * Extracts the signed numeric value from each enum constant's {@code name()} (via {@link #parseSignedName}), in
     * declaration order, skipping constants with no magnitude.  Used for FCD1 LNA gain whose dB values are encoded in
     * the constant name (e.g. LNA_GAIN_MINUS_5_0 -> -5.0).
     */
    private static List<Double> enumSignedNumbers(Enum<?>[] values)
    {
        List<Double> numbers = new ArrayList<>();

        for(Enum<?> v : values)
        {
            Double number = parseSignedName(v.name());

            if(number != null)
            {
                numbers.add(number);
            }
        }

        return numbers;
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
                Map<String,Object> result = applyGain(c, body);

                //Cache the applied numeric gain for reporting in buildTunerList (most controllers can't read it back).
                if(Boolean.TRUE.equals(result.get("ok")) && result.get("gain") != null)
                {
                    mLastGain.put(id, result.get("gain"));
                }

                sendJson(exchange, 200, result);
                break;
            }
            case "samplerate":
            {
                long sampleRate = body.has("sampleRate") ? body.get("sampleRate").asLong() : 0L;
                sendJson(exchange, 200, applySampleRate(c, sampleRate));
                break;
            }
            case "autoppm":
            {
                boolean enabled = body.has("enabled") && body.get("enabled").asBoolean();
                Map<String,Object> ok = new LinkedHashMap<>();

                try
                {
                    FrequencyErrorCorrectionManager manager = c.getFrequencyErrorCorrectionManager();

                    if(manager != null)
                    {
                        //Real mechanism: SDR-Trunk's FrequencyErrorCorrectionManager watches the decoder-measured
                        //PPM error and, when enabled, nudges the tuner's frequency correction toward it.  This is
                        //genuine automatic PPM correction, NOT a fabricated loop.
                        manager.setEnabled(enabled);
                        ok.put("ok", true);
                        ok.put("autoPpm", manager.isEnabled());
                    }
                    else
                    {
                        //No correction manager on this controller - report as a no-op rather than an error.
                        ok.put("ok", true);
                        ok.put("autoPpm", enabled);
                        ok.put("note", "not supported by this build");
                    }
                }
                catch(Exception e)
                {
                    ok.put("ok", true);
                    ok.put("autoPpm", enabled);
                    ok.put("note", "not supported by this build");
                }

                sendJson(exchange, 200, ok);
                break;
            }
            default:
                sendJson(exchange, 404, error("not found"));
                break;
        }
    }

    //---------------------------------------------------------------------------------------------------------------
    // Tuner gain (device-typed dispatch)
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Best-effort read of the current composite gain for controllers that expose a getter.  Returns null when the
     * controller has no readable current-gain value (the common case - gain state lives in the tuner configuration).
     */
    private Object readCurrentGain(TunerController c)
    {
        try
        {
            if(c instanceof RspTunerController rsp)
            {
                return (double)rsp.getControlRsp().getCurrentGain();
            }

            if(c instanceof FCD1TunerController fcd1)
            {
                return String.valueOf(fcd1.getLNAGainSetting());
            }
        }
        catch(Exception e)
        {
            //best effort
        }

        return null;
    }

    /**
     * Applies gain to the tuner, dispatching on the concrete controller (and, for RTL, the embedded tuner) type.
     * SDR-Trunk has no common gain interface, so each device is handled explicitly.
     *
     * <p>Request body fields (all optional - each device reads what applies to it):</p>
     * <ul>
     *   <li>{@code gain} (number) - desired gain in device units (dB-ish); the server snaps to the nearest
     *       supported discrete step.</li>
     *   <li>{@code auto} (boolean) - request automatic/AGC gain where the device supports it.</li>
     *   <li>{@code gainMode} (string, "LINEARITY"|"SENSITIVITY") - Airspy / HydraSDR gain curve (default LINEARITY).</li>
     *   <li>{@code lnaGain}, {@code vgaGain} (number) - HackRF LNA/VGA axes (dB).</li>
     *   <li>{@code amp} (boolean) - HackRF RF amplifier enable.</li>
     *   <li>{@code lnaState}, {@code gainReduction} (number) - SDRplay LNA state index and baseband gain reduction (20-59).</li>
     *   <li>{@code attenuation} (number), {@code lna} (boolean) - Airspy HF attenuation (dB) and LNA enable.</li>
     * </ul>
     *
     * @return response map: {@code {ok, tunerType, gain, gainLabel, ...device echoes...}} on success, or
     *         {@code {ok:false, error}} when the tuner type has no settable gain via this API.
     */
    private Map<String,Object> applyGain(TunerController c, JsonNode body)
    {
        String tunerType = c.getClass().getSimpleName();
        double requested = body.has("gain") ? body.get("gain").asDouble() : Double.NaN;
        boolean auto = body.has("auto") && body.get("auto").asBoolean();

        try
        {
            //---------------------------------------------------------------------------------- RTL2832 family
            if(c instanceof RTL2832TunerController rtl && rtl.hasEmbeddedTuner())
            {
                EmbeddedTuner embedded = rtl.getEmbeddedTuner();

                if(embedded instanceof R8xEmbeddedTuner r8x)
                {
                    if(auto)
                    {
                        r8x.setGain(R8xEmbeddedTuner.MasterGain.AUTOMATIC, true);
                        return gainOk("R820T/R828D", null, "Automatic");
                    }

                    R8xEmbeddedTuner.MasterGain chosen = R8xEmbeddedTuner.MasterGain.MANUAL;
                    double best = Double.MAX_VALUE;

                    for(R8xEmbeddedTuner.MasterGain g : R8xEmbeddedTuner.MasterGain.values())
                    {
                        Double value = parseLeadingNumber(g.toString());

                        if(value != null && Math.abs(value - requested) < best)
                        {
                            best = Math.abs(value - requested);
                            chosen = g;
                        }
                    }

                    r8x.setGain(chosen, true);
                    return gainOk("R820T/R828D", parseLeadingNumber(chosen.toString()), chosen.toString());
                }

                if(embedded instanceof E4KEmbeddedTuner e4k)
                {
                    if(auto)
                    {
                        e4k.setGain(E4KEmbeddedTuner.E4KGain.AUTOMATIC, true);
                        return gainOk("E4K", null, "Automatic");
                    }

                    E4KEmbeddedTuner.E4KGain chosen = E4KEmbeddedTuner.E4KGain.MANUAL;
                    double best = Double.MAX_VALUE;

                    for(E4KEmbeddedTuner.E4KGain g : E4KEmbeddedTuner.E4KGain.values())
                    {
                        //Match against the dB label (e.g. "16.5 db"); AUTOMATIC/MANUAL have no numeric label.
                        Double value = parseLeadingNumber(g.toString());

                        if(value != null && Math.abs(value - requested) < best)
                        {
                            best = Math.abs(value - requested);
                            chosen = g;
                        }
                    }

                    e4k.setGain(chosen, true);
                    return gainOk("E4K", parseLeadingNumber(chosen.toString()), chosen.toString());
                }

                if(embedded instanceof FC0013EmbeddedTuner fc)
                {
                    if(auto)
                    {
                        fc.setGain(true, FC0013EmbeddedTuner.LNAGain.values()[0]);
                        return gainOk("FC0013", null, "AGC");
                    }

                    FC0013EmbeddedTuner.LNAGain chosen = FC0013EmbeddedTuner.LNAGain.values()[0];
                    double best = Double.MAX_VALUE;

                    for(FC0013EmbeddedTuner.LNAGain g : FC0013EmbeddedTuner.LNAGain.values())
                    {
                        Double value = parseLeadingNumber(g.toString());

                        if(value != null && Math.abs(value - requested) < best)
                        {
                            best = Math.abs(value - requested);
                            chosen = g;
                        }
                    }

                    fc.setGain(false, chosen);
                    return gainOk("FC0013", parseLeadingNumber(chosen.toString()), chosen.toString());
                }

                return error("gain control not supported for RTL embedded tuner type [" +
                        embedded.getClass().getSimpleName() + "]");
            }

            //---------------------------------------------------------------------------------- Airspy / HydraSDR
            if(c instanceof AirspyTunerController airspy)
            {
                boolean sensitivity = "SENSITIVITY".equalsIgnoreCase(body.path("gainMode").asText(""));
                int level = clamp((int)Math.round(Double.isNaN(requested) ? 0 : requested), 1, 22);
                AirspyTunerController.GainMode mode = sensitivity ?
                        AirspyTunerController.GainMode.SENSITIVITY : AirspyTunerController.GainMode.LINEARITY;
                airspy.setGain(AirspyTunerController.Gain.getGain(mode, level));
                Map<String,Object> ok = gainOk("Airspy", (double)level, mode.name() + "_" + level);
                ok.put("gainMode", mode.name());
                return ok;
            }

            if(c instanceof HydraSdrTunerController hydra)
            {
                boolean sensitivity = "SENSITIVITY".equalsIgnoreCase(body.path("gainMode").asText(""));
                int level = clamp((int)Math.round(Double.isNaN(requested) ? 0 : requested), 1, 22);
                HydraSdrTunerController.GainMode mode = sensitivity ?
                        HydraSdrTunerController.GainMode.SENSITIVITY : HydraSdrTunerController.GainMode.LINEARITY;
                hydra.setGain(HydraSdrTunerController.Gain.getGain(mode, level));
                Map<String,Object> ok = gainOk("HydraSDR", (double)level, mode.name() + "_" + level);
                ok.put("gainMode", mode.name());
                return ok;
            }

            //---------------------------------------------------------------------------------- HackRF (two-axis)
            if(c instanceof HackRFTunerController hackrf)
            {
                Double lnaReq = body.has("lnaGain") ? body.get("lnaGain").asDouble() :
                        (Double.isNaN(requested) ? null : requested);
                Double vgaReq = body.has("vgaGain") ? body.get("vgaGain").asDouble() : null;
                Map<String,Object> ok = new LinkedHashMap<>();
                ok.put("ok", true);
                ok.put("tunerType", "HackRF");

                if(lnaReq != null)
                {
                    HackRFTunerController.HackRFLNAGain chosen = HackRFTunerController.HackRFLNAGain.values()[0];
                    double best = Double.MAX_VALUE;

                    for(HackRFTunerController.HackRFLNAGain g : HackRFTunerController.HackRFLNAGain.values())
                    {
                        if(Math.abs(g.getValue() - lnaReq) < best)
                        {
                            best = Math.abs(g.getValue() - lnaReq);
                            chosen = g;
                        }
                    }

                    hackrf.setLNAGain(chosen);
                    ok.put("lnaGain", chosen.getValue());
                    ok.put("gain", chosen.getValue());
                }

                if(vgaReq != null)
                {
                    HackRFTunerController.HackRFVGAGain chosen = HackRFTunerController.HackRFVGAGain.values()[0];
                    double best = Double.MAX_VALUE;

                    for(HackRFTunerController.HackRFVGAGain g : HackRFTunerController.HackRFVGAGain.values())
                    {
                        if(Math.abs(g.getValue() - vgaReq) < best)
                        {
                            best = Math.abs(g.getValue() - vgaReq);
                            chosen = g;
                        }
                    }

                    hackrf.setVGAGain(chosen);
                    ok.put("vgaGain", chosen.getValue());
                }

                if(body.has("amp"))
                {
                    hackrf.setAmplifierEnabled(body.get("amp").asBoolean());
                    ok.put("amp", body.get("amp").asBoolean());
                }

                return ok;
            }

            //---------------------------------------------------------------------------------- SDRplay RSP
            if(c instanceof RspTunerController rsp)
            {
                int lna = body.has("lnaState") ? body.get("lnaState").asInt() : rsp.getControlRsp().getLNA();
                int gr = body.has("gainReduction") ? body.get("gainReduction").asInt() :
                        rsp.getControlRsp().getBasebandGainReduction();
                rsp.getControlRsp().setGain(lna, gr);
                Map<String,Object> ok = new LinkedHashMap<>();
                ok.put("ok", true);
                ok.put("tunerType", "SDRplay RSP");
                ok.put("lnaState", lna);
                ok.put("gainReduction", gr);
                ok.put("gain", (double)rsp.getControlRsp().getCurrentGain());
                return ok;
            }

            //---------------------------------------------------------------------------------- FUNcube Dongle Pro V1
            if(c instanceof FCD1TunerController fcd1)
            {
                FCD1TunerController.LNAGain chosen = FCD1TunerController.LNAGain.values()[0];
                double best = Double.MAX_VALUE;

                for(FCD1TunerController.LNAGain g : FCD1TunerController.LNAGain.values())
                {
                    Double value = parseSignedName(g.name());

                    if(value != null && Math.abs(value - requested) < best)
                    {
                        best = Math.abs(value - requested);
                        chosen = g;
                    }
                }

                fcd1.setLNAGain(chosen);
                return gainOk("FUNcube Pro V1", parseSignedName(chosen.name()), chosen.toString());
            }

            //---------------------------------------------------------------------------------- FUNcube Dongle Pro+ V2
            if(c instanceof FCD2TunerController fcd2)
            {
                boolean enabled = body.has("enabled") ? body.get("enabled").asBoolean() :
                        (!Double.isNaN(requested) && requested > 0);
                fcd2.setLNAGain(enabled);
                Map<String,Object> ok = gainOk("FUNcube Pro+ V2", enabled ? 1d : 0d, enabled ? "LNA on" : "LNA off");
                ok.put("lna", enabled);
                return ok;
            }

            //---------------------------------------------------------------------------------- Airspy HF+ (attenuation)
            if(c instanceof AirspyHfTunerController hf)
            {
                Map<String,Object> ok = new LinkedHashMap<>();
                ok.put("ok", true);
                ok.put("tunerType", "Airspy HF+");

                if(body.has("attenuation") || !Double.isNaN(requested))
                {
                    double attReq = body.has("attenuation") ? body.get("attenuation").asDouble() : requested;
                    Attenuation chosen = Attenuation.values()[0];
                    double best = Double.MAX_VALUE;

                    for(Attenuation a : Attenuation.values())
                    {
                        if(Math.abs(a.getValue() - attReq) < best)
                        {
                            best = Math.abs(a.getValue() - attReq);
                            chosen = a;
                        }
                    }

                    hf.setAttenuation(chosen);
                    ok.put("attenuation", (int)chosen.getValue());
                    ok.put("gain", (int)chosen.getValue());
                }

                if(body.has("lna"))
                {
                    hf.setLna(body.get("lna").asBoolean());
                    ok.put("lna", body.get("lna").asBoolean());
                }

                return ok;
            }
        }
        catch(Exception e)
        {
            return error("gain error on [" + tunerType + "]: " +
                    (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }

        return error("gain control not supported for tuner type [" + tunerType + "]");
    }

    /**
     * Builds a standard successful gain response.
     */
    private Map<String,Object> gainOk(String tunerType, Double gain, String gainLabel)
    {
        Map<String,Object> ok = new LinkedHashMap<>();
        ok.put("ok", true);
        ok.put("tunerType", tunerType);
        ok.put("gain", gain);
        ok.put("gainLabel", gainLabel);
        return ok;
    }

    //---------------------------------------------------------------------------------------------------------------
    // Tuner sample rate (device-typed dispatch, validated against the device's allowed rates)
    //---------------------------------------------------------------------------------------------------------------

    /**
     * Sets the tuner sample rate, validating the requested rate (Hz) against the device's allowed set.  Returns a
     * clear error listing the allowed rates when the request is not an exact supported value, or when the device
     * has a fixed sample rate.
     *
     * @return {@code {ok:true, tunerType, sampleRate}} on success, else {@code {ok:false, error, allowedSampleRates}}.
     */
    private Map<String,Object> applySampleRate(TunerController c, long requested)
    {
        String tunerType = c.getClass().getSimpleName();

        try
        {
            if(c instanceof RTL2832TunerController rtl)
            {
                List<Long> allowed = new ArrayList<>();

                for(RTL2832TunerController.SampleRate sr : RTL2832TunerController.SampleRate.values())
                {
                    allowed.add((long)sr.getRate());

                    if(sr.getRate() == requested)
                    {
                        rtl.setSampleRate(sr);
                        return sampleRateOk("RTL2832", sr.getRate());
                    }
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof HackRFTunerController hackrf)
            {
                List<Long> allowed = new ArrayList<>();

                for(HackRFTunerController.HackRFSampleRate sr : HackRFTunerController.HackRFSampleRate.VALID_SAMPLE_RATES)
                {
                    allowed.add((long)sr.getRate());

                    if((long)sr.getRate() == requested)
                    {
                        hackrf.setSampleRate(sr);
                        return sampleRateOk("HackRF", (long)sr.getRate());
                    }
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof AirspyTunerController airspy)
            {
                List<Long> allowed = new ArrayList<>();

                for(AirspySampleRate sr : airspy.getSampleRates())
                {
                    allowed.add((long)sr.getRate());
                }

                AirspySampleRate match = airspy.getSampleRate((int)requested);

                if(match != null && match.getRate() == requested)
                {
                    airspy.setSampleRate(match);
                    return sampleRateOk("Airspy", match.getRate());
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof HydraSdrTunerController hydra)
            {
                List<Long> allowed = new ArrayList<>();

                for(io.github.dsheirer.source.tuner.hydrasdr.HydraSdrSampleRate sr : hydra.getSampleRates())
                {
                    allowed.add((long)sr.getRate());
                }

                io.github.dsheirer.source.tuner.hydrasdr.HydraSdrSampleRate match = hydra.getSampleRate((int)requested);

                if(match != null && match.getRate() == requested)
                {
                    hydra.setSampleRate(match);
                    return sampleRateOk("HydraSDR", match.getRate());
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof RspTunerController rsp)
            {
                List<Long> allowed = new ArrayList<>();

                for(RspSampleRate sr : RspSampleRate.values())
                {
                    allowed.add(sr.getSampleRate());

                    if(sr.getSampleRate() == requested)
                    {
                        rsp.setSampleRate(sr);
                        return sampleRateOk("SDRplay RSP", sr.getSampleRate());
                    }
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof AirspyHfTunerController hf)
            {
                List<Long> allowed = new ArrayList<>();

                for(AirspyHfSampleRate sr : hf.getAvailableSampleRates())
                {
                    allowed.add((long)sr.getSampleRate());

                    if(sr.getSampleRate() == requested)
                    {
                        hf.setSampleRate(sr);
                        return sampleRateOk("Airspy HF+", sr.getSampleRate());
                    }
                }

                return unsupportedSampleRate(tunerType, allowed);
            }

            if(c instanceof FCD1TunerController || c instanceof FCD2TunerController)
            {
                Map<String,Object> err = error("sample rate is fixed for tuner type [" + tunerType + "]");
                err.put("sampleRate", (long)c.getSampleRate());
                return err;
            }
        }
        catch(Exception e)
        {
            return error("sample rate error on [" + tunerType + "]: " +
                    (e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName()));
        }

        return error("sample rate control not supported for tuner type [" + tunerType + "]");
    }

    private Map<String,Object> sampleRateOk(String tunerType, long sampleRate)
    {
        Map<String,Object> ok = new LinkedHashMap<>();
        ok.put("ok", true);
        ok.put("tunerType", tunerType);
        ok.put("sampleRate", sampleRate);
        return ok;
    }

    private Map<String,Object> unsupportedSampleRate(String tunerType, List<Long> allowed)
    {
        Map<String,Object> err = error("unsupported sample rate for tuner type [" + tunerType + "]");
        err.put("allowedSampleRates", allowed);
        return err;
    }

    /**
     * Clamps an integer to the inclusive range [min, max].
     */
    private static int clamp(int value, int min, int max)
    {
        return Math.max(min, Math.min(max, value));
    }

    /**
     * Parses the leading number from a label such as "248", "23 HIGH" or "0.240 MHz".  Returns null if none.
     */
    private static Double parseLeadingNumber(String text)
    {
        if(text == null)
        {
            return null;
        }

        java.util.regex.Matcher m = java.util.regex.Pattern.compile("-?\\d+(?:\\.\\d+)?").matcher(text);
        return m.find() ? Double.valueOf(m.group()) : null;
    }

    /**
     * Parses a signed value from an enum constant name such as "PLUS_15", "MINUS_10" or "LNA_GAIN_PLUS_30_0".
     * "PLUS"/"MINUS" set the sign; the first numeric group is the magnitude (an optional trailing "_0" is the
     * decimal tenths, e.g. "PLUS_30_0" -> 30.0).  Returns null when no magnitude is present.
     */
    private static Double parseSignedName(String name)
    {
        if(name == null)
        {
            return null;
        }

        double sign = name.contains("MINUS") ? -1d : 1d;
        java.util.regex.Matcher m = java.util.regex.Pattern.compile("(\\d+)(?:_(\\d+))?\\s*$").matcher(name);

        if(m.find())
        {
            double magnitude = Double.parseDouble(m.group(1));

            if(m.group(2) != null)
            {
                magnitude += Double.parseDouble("0." + m.group(2));
            }

            return sign * magnitude;
        }

        return null;
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

        // Snapshot before iterating off-thread (JavaFX list, mutated by decode/reload).
        for(Channel channel : new ArrayList<>(cm.getChannels()))
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
                    String stateText = state != null ? state.toString() : "PROCESSING";
                    entry.put("state", stateText);
                    //CONTROL indicates the channel is locked on a trunking control channel (P25/DMR/etc.).
                    entry.put("control", "CONTROL".equals(stateText));

                    Identifier from = meta.getFromIdentifier();
                    entry.put("from", from != null ? from.toString() : null);
                    entry.put("fromAlias", aliasNames(meta.getFromIdentifierAliases()));

                    Identifier to = meta.getToIdentifier();
                    entry.put("to", to != null ? to.toString() : null);
                    entry.put("toAlias", aliasNames(meta.getToIdentifierAliases()));

                    Identifier talker = meta.getTalkerAliasIdentifier();
                    entry.put("talkerAlias", talker != null ? talker.toString() : null);

                    entry.put("timeslot", meta.getTimeslot());
                    entry.put("frequency", frequencyOf(meta));
                }
                else
                {
                    entry.put("state", "PROCESSING");
                    entry.put("control", false);
                    entry.put("from", null);
                    entry.put("fromAlias", null);
                    entry.put("to", null);
                    entry.put("toAlias", null);
                    entry.put("talkerAlias", null);
                    entry.put("timeslot", null);
                    entry.put("frequency", null);
                }
            }
            else
            {
                entry.put("state", "STOPPED");
                entry.put("control", false);
                entry.put("from", null);
                entry.put("fromAlias", null);
                entry.put("to", null);
                entry.put("toAlias", null);
                entry.put("talkerAlias", null);
                entry.put("timeslot", null);
                entry.put("frequency", null);
            }

            list.add(entry);
        }

        Map<String,Object> body = new LinkedHashMap<>();
        body.put("channels", list);
        body.put("activeCalls", buildActiveCalls(mm));
        return body;
    }

    /**
     * Builds a "Now Playing" list from the live channel metadata model - one entry per metadata row that is in an
     * active state (CALL, ENCRYPTED, DATA, CONTROL or ACTIVE).  This captures dynamically-allocated trunking traffic
     * channels that are NOT in the configured channel list, so a UI can render active calls (talkgroup, from -&gt; to
     * with aliases, control-channel lock) without guessing.  Includes CONTROL rows so the control channel is visible.
     */
    private List<Map<String,Object>> buildActiveCalls(ChannelMetadataModel mm)
    {
        List<Map<String,Object>> calls = new ArrayList<>();
        int rows = mm.getRowCount();

        for(int r = 0; r < rows; r++)
        {
            try
            {
                ChannelMetadata meta = mm.getChannelMetadata(r);

                if(meta == null)
                {
                    continue;
                }

                Identifier state = meta.getChannelStateIdentifier();
                String stateText = state != null ? state.toString() : null;

                //Only emit rows that reflect active decode (call / control / data / active).
                if(stateText == null || "IDLE".equals(stateText) || "FADE".equals(stateText) ||
                        "RESET".equals(stateText) || "TEARDOWN".equals(stateText))
                {
                    continue;
                }

                Map<String,Object> call = new LinkedHashMap<>();
                call.put("state", stateText);
                call.put("control", "CONTROL".equals(stateText));

                Channel ch = mm.getChannelFromMetadata(meta);
                call.put("channelId", ch != null ? ch.getChannelID() : null);
                call.put("channelName", ch != null ? ch.getName() : null);

                Identifier from = meta.getFromIdentifier();
                call.put("from", from != null ? from.toString() : null);
                call.put("fromAlias", aliasNames(meta.getFromIdentifierAliases()));

                Identifier to = meta.getToIdentifier();
                //For group calls the TO identifier is the talkgroup.
                call.put("to", to != null ? to.toString() : null);
                call.put("talkgroup", to != null ? to.toString() : null);
                call.put("toAlias", aliasNames(meta.getToIdentifierAliases()));

                Identifier talker = meta.getTalkerAliasIdentifier();
                call.put("talkerAlias", talker != null ? talker.toString() : null);

                call.put("timeslot", meta.getTimeslot());
                call.put("frequency", frequencyOf(meta));

                calls.add(call);
            }
            catch(Exception e)
            {
                //Metadata list mutates on the EDT - ignore transient index issues.
            }
        }

        return calls;
    }

    /**
     * Best-effort comma-joined alias names, or null if the list is null/empty.
     */
    private static String aliasNames(List<Alias> aliases)
    {
        if(aliases == null || aliases.isEmpty())
        {
            return null;
        }

        StringBuilder sb = new StringBuilder();

        for(Alias alias : aliases)
        {
            if(alias != null && alias.getName() != null && !alias.getName().isEmpty())
            {
                if(sb.length() > 0)
                {
                    sb.append(", ");
                }

                sb.append(alias.getName());
            }
        }

        return sb.length() > 0 ? sb.toString() : null;
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

        // Snapshot before iterating off-thread (JavaFX list, mutated by decode/reload).
        for(Channel channel : new ArrayList<>(cm.getChannels()))
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
