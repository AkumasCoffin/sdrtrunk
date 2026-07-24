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

import io.github.dsheirer.buffer.INativeBuffer;
import io.github.dsheirer.source.tuner.Tuner;
import io.github.dsheirer.source.tuner.TunerController;
import io.github.dsheirer.spectrum.ComplexDftProcessor;
import io.github.dsheirer.spectrum.DFTResultsListener;
import io.github.dsheirer.spectrum.DFTSize;
import io.github.dsheirer.spectrum.converter.ComplexDecibelConverter;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.java_websocket.WebSocket;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Taps a single tuner's complex sample stream, runs an FFT + dB conversion, and emits little-endian binary spectrum
 * frames to a single owning WebSocket connection.
 *
 * Binary frame layout (little-endian):
 *   u8  type = 0x01
 *   u8  tunerIndex
 *   u64 centerFreqHz
 *   u32 sampleRateHz
 *   u16 binCount
 *   int8[binCount] dB   (clamped/rounded to [-128, 127], DC-centered, low-freq edge = index 0)
 */
public class SpectrumStreamer implements DFTResultsListener
{
    private static final Logger mLog = LoggerFactory.getLogger(SpectrumStreamer.class);
    private static final byte FRAME_TYPE_SPECTRUM = 0x01;
    private static final int HEADER_BYTES = 1 + 1 + 8 + 4 + 2;

    private final Tuner mTuner;
    private final int mTunerIndex;
    private final WebSocket mConnection;

    private ComplexDftProcessor<INativeBuffer> mProcessor;
    private ComplexDecibelConverter mConverter;
    private volatile boolean mRunning;

    /**
     * Constructs a spectrum streamer bound to a tuner and an owning WebSocket connection.
     * @param tuner to tap for complex sample buffers.
     * @param tunerIndex position of the tuner within the available tuners list (emitted in each frame).
     * @param connection to send binary frames to.
     */
    public SpectrumStreamer(Tuner tuner, int tunerIndex, WebSocket connection)
    {
        mTuner = tuner;
        mTunerIndex = tunerIndex;
        mConnection = connection;
    }

    /**
     * Maps a requested bin count to the nearest supported DFT size (512 / 1024 / 2048).
     */
    public static DFTSize mapSize(int bins)
    {
        if(bins <= 768)
        {
            return DFTSize.FFT00512;
        }
        else if(bins <= 1536)
        {
            return DFTSize.FFT01024;
        }

        return DFTSize.FFT02048;
    }

    /**
     * Starts the FFT tap and begins emitting frames.
     * @param bins requested bin count (mapped to nearest DFT size).
     * @param fps requested frame rate (clamped to 1..30).
     */
    public synchronized void start(int bins, int fps)
    {
        if(mRunning)
        {
            return;
        }

        int frameRate = Math.max(1, Math.min(30, fps));

        mProcessor = new ComplexDftProcessor<>();
        mProcessor.setDFTSize(mapSize(bins));
        mProcessor.setFrameRate(frameRate);

        mConverter = new ComplexDecibelConverter();
        mProcessor.addConverter(mConverter);
        mConverter.addListener(this);

        mTuner.getTunerController().addBufferListener(mProcessor);
        mRunning = true;
    }

    @Override
    public void receive(float[] dbBins)
    {
        if(mRunning && dbBins != null)
        {
            emitFrame(dbBins);
        }
    }

    /**
     * Builds and sends a single binary spectrum frame.
     */
    private void emitFrame(float[] dbBins)
    {
        try
        {
            if(mConnection == null || !mConnection.isOpen())
            {
                return;
            }

            TunerController controller = mTuner.getTunerController();
            long centerFreq = controller.getFrequency();
            int sampleRate = (int)controller.getSampleRate();
            int binCount = dbBins.length;

            ByteBuffer buffer = ByteBuffer.allocate(HEADER_BYTES + binCount).order(ByteOrder.LITTLE_ENDIAN);
            buffer.put(FRAME_TYPE_SPECTRUM);
            buffer.put((byte)(mTunerIndex & 0xFF));
            buffer.putLong(centerFreq);
            buffer.putInt(sampleRate);
            buffer.putShort((short)(binCount & 0xFFFF));

            for(int x = 0; x < binCount; x++)
            {
                int value = Math.round(dbBins[x]);

                if(value > 127)
                {
                    value = 127;
                }
                else if(value < -128)
                {
                    value = -128;
                }

                buffer.put((byte)value);
            }

            mConnection.send(buffer.array());
        }
        catch(Exception e)
        {
            //Connection may have closed mid-send - stop quietly.
            mLog.debug("Error sending spectrum frame - detaching streamer", e);
        }
    }

    /**
     * Stops the FFT tap and releases resources.
     */
    public synchronized void stop()
    {
        if(!mRunning)
        {
            return;
        }

        mRunning = false;

        try
        {
            mTuner.getTunerController().removeBufferListener(mProcessor);
        }
        catch(Exception e)
        {
            mLog.debug("Error removing buffer listener during spectrum stream stop", e);
        }

        if(mConverter != null)
        {
            mConverter.removeListener(this);
            mConverter.dispose();
            mConverter = null;
        }

        if(mProcessor != null)
        {
            mProcessor.stop();
            mProcessor.dispose();
            mProcessor = null;
        }
    }
}
