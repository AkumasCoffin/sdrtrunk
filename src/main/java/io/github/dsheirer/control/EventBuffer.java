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

import io.github.dsheirer.alias.Alias;
import io.github.dsheirer.alias.AliasList;
import io.github.dsheirer.alias.AliasModel;
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
 *
 * <p>Each buffered {@link EventEntry} carries the call start/end timestamps, duration, event type (both the enum
 * constant name and the human-readable label), protocol, timeslot, the FROM and TO identifiers, and - when an
 * {@link AliasModel} is available - the resolved FROM/TO alias names.  This is sufficient for a downstream UI to
 * render an "Events" log and a "Now Playing" list (talkgroup = TO, caller = FROM, plus aliases and duration)
 * without additional lookups.</p>
 */
public class EventBuffer implements Listener<IDecodeEvent>
{
    private static final int DEFAULT_CAPACITY = 500;

    private final int mCapacity;
    private final AliasModel mAliasModel;
    private final Deque<EventEntry> mEvents = new ArrayDeque<>();
    private final Object mLock = new Object();

    /**
     * Constructs an event buffer with the default capacity (500) and no alias resolution.
     */
    public EventBuffer()
    {
        this(DEFAULT_CAPACITY, null);
    }

    /**
     * Constructs an event buffer with the default capacity (500).
     * @param aliasModel for resolving FROM/TO alias names, or null to disable alias resolution.
     */
    public EventBuffer(AliasModel aliasModel)
    {
        this(DEFAULT_CAPACITY, aliasModel);
    }

    /**
     * Constructs an event buffer with the specified capacity.
     * @param capacity maximum number of events to retain.
     * @param aliasModel for resolving FROM/TO alias names, or null to disable alias resolution.
     */
    public EventBuffer(int capacity, AliasModel aliasModel)
    {
        mCapacity = capacity > 0 ? capacity : DEFAULT_CAPACITY;
        mAliasModel = aliasModel;
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
        entry.timeStart = event.getTimeStart();
        entry.timeEnd = event.getTimeEnd();
        entry.durationMs = event.getDuration();

        try
        {
            entry.timeslot = event.getTimeslot();
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        try
        {
            if(event.getEventType() != null)
            {
                entry.type = event.getEventType().name();
                entry.typeLabel = event.getEventType().getLabel();
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

        try
        {
            entry.details = event.getDetails();
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        IdentifierCollection identifiers = event.getIdentifierCollection();

        if(identifiers != null)
        {
            Identifier fromIdentifier = firstIdentifierOf(identifiers, Role.FROM);
            Identifier toIdentifier = firstIdentifierOf(identifiers, Role.TO);
            entry.from = fromIdentifier != null ? fromIdentifier.toString() : null;
            entry.to = toIdentifier != null ? toIdentifier.toString() : null;

            //Resolve aliases via the alias list named by this collection, best-effort.
            AliasList aliasList = resolveAliasList(identifiers);

            if(aliasList != null)
            {
                entry.fromAlias = aliasNames(aliasList, fromIdentifier);
                entry.toAlias = aliasNames(aliasList, toIdentifier);
            }
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

    private AliasList resolveAliasList(IdentifierCollection collection)
    {
        if(mAliasModel == null)
        {
            return null;
        }

        try
        {
            return mAliasModel.getAliasList(collection);
        }
        catch(Exception e)
        {
            //ignore - best effort
            return null;
        }
    }

    /**
     * Best-effort comma-joined alias names matching the identifier, or null if none.
     */
    private static String aliasNames(AliasList aliasList, Identifier identifier)
    {
        if(aliasList == null || identifier == null)
        {
            return null;
        }

        try
        {
            List<Alias> aliases = aliasList.getAliases(identifier);

            if(aliases != null && !aliases.isEmpty())
            {
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
        }
        catch(Exception e)
        {
            //ignore - best effort
        }

        return null;
    }

    /**
     * Best-effort extraction of the first identifier with the given role.
     */
    private static Identifier firstIdentifierOf(IdentifierCollection collection, Role role)
    {
        try
        {
            List<Identifier> matches = collection.getIdentifiers(role);

            if(matches != null && !matches.isEmpty())
            {
                return matches.get(0);
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
        /** Legacy alias for {@link #timeStart} (epoch millis).  Retained for backward compatibility. */
        public long at;
        /** Call/event start time, epoch millis. */
        public long timeStart;
        /** Call/event end time, epoch millis.  0 while the event/call is still in progress. */
        public long timeEnd;
        /** Event type enum constant name, e.g. "CALL_GROUP", "CALL_END", "REGISTER". */
        public String type;
        /** Human-readable event type label, e.g. "Group Call", "Call End". */
        public String typeLabel;
        /** Protocol enum name, e.g. "APCO25", "DMR". */
        public String protocol;
        /** FROM (caller) identifier as text, e.g. a radio id. May be null. */
        public String from;
        /** Resolved FROM alias name(s), comma-joined, or null if none / unresolved. */
        public String fromAlias;
        /** TO identifier as text - the talkgroup for group calls, or a radio id for unit-to-unit. May be null. */
        public String to;
        /** Resolved TO alias name(s), comma-joined, or null if none / unresolved. */
        public String toAlias;
        /** Event duration in millis (getDuration()). */
        public long durationMs;
        /** Decoder timeslot (0 for single-timeslot protocols, 0/1 for DMR / P25 Phase 2). */
        public int timeslot;
        /** Free-text detail string from the decoder, may be null. */
        public String details;
        /** Channel descriptor text (frequency / logical channel), may be null. */
        public String channel;
    }
}
