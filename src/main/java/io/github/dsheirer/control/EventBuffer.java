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

import io.github.dsheirer.channel.IChannelDescriptor;
import io.github.dsheirer.identifier.Identifier;
import io.github.dsheirer.identifier.IdentifierCollection;
import io.github.dsheirer.identifier.Role;
import io.github.dsheirer.module.decode.event.IDecodeEvent;
import io.github.dsheirer.sample.Listener;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * Fixed-size ring buffer of recent decode events for the headless control server.  Registered as a decode event
 * listener on the {@link io.github.dsheirer.controller.channel.ChannelProcessingManager}.
 */
public class EventBuffer implements Listener<IDecodeEvent>
{
    private static final int DEFAULT_CAPACITY = 500;

    private final int mCapacity;
    private final Deque<EventEntry> mEvents = new ArrayDeque<>();
    private final Object mLock = new Object();

    /**
     * Constructs an event buffer with the default capacity (500).
     */
    public EventBuffer()
    {
        this(DEFAULT_CAPACITY);
    }

    /**
     * Constructs an event buffer with the specified capacity.
     * @param capacity maximum number of events to retain.
     */
    public EventBuffer(int capacity)
    {
        mCapacity = capacity > 0 ? capacity : DEFAULT_CAPACITY;
    }

    @Override
    public void receive(IDecodeEvent event)
    {
        if(event == null)
        {
            return;
        }

        EventEntry entry = new EventEntry();
        entry.at = event.getTimeStart();
        entry.durationMs = event.getDuration();

        try
        {
            if(event.getEventType() != null)
            {
                entry.type = event.getEventType().name();
            }
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        try
        {
            if(event.getProtocol() != null)
            {
                entry.protocol = event.getProtocol().name();
            }
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        IdentifierCollection identifiers = event.getIdentifierCollection();

        if(identifiers != null)
        {
            entry.from = firstIdentifier(identifiers, Role.FROM);
            entry.to = firstIdentifier(identifiers, Role.TO);
        }

        IChannelDescriptor descriptor = event.getChannelDescriptor();

        if(descriptor != null)
        {
            entry.channel = descriptor.toString();
        }

        synchronized(mLock)
        {
            mEvents.addLast(entry);

            while(mEvents.size() > mCapacity)
            {
                mEvents.removeFirst();
            }
        }
    }

    /**
     * Best-effort extraction of the first identifier with the given role.
     */
    private static String firstIdentifier(IdentifierCollection collection, Role role)
    {
        try
        {
            List<Identifier> matches = collection.getIdentifiers(role);

            if(matches != null && !matches.isEmpty())
            {
                Identifier identifier = matches.get(0);
                return identifier != null ? identifier.toString() : null;
            }
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        return null;
    }

    /**
     * Returns the newest {@code limit} events, oldest first (newest last).
     * @param limit maximum number of events to return.
     * @return list of event entries.
     */
    public List<EventEntry> getEvents(int limit)
    {
        synchronized(mLock)
        {
            int size = mEvents.size();
            int count = (limit > 0 && limit < size) ? limit : size;
            List<EventEntry> all = new ArrayList<>(mEvents);
            //Newest last - return the tail 'count' entries.
            return new ArrayList<>(all.subList(size - count, size));
        }
    }

    /**
     * Single decode event snapshot.  Public fields are serialized directly by Jackson.
     */
    public static class EventEntry
    {
        public long at;
        public String type;
        public String protocol;
        public String from;
        public String to;
        public long durationMs;
        public String channel;
    }
}
