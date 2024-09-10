/**
 *   This file is part of Skript.
 *
 *  Skript is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 *
 *  Skript is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with Skript.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright Peter Güttinger, SkriptLang team and contributors
 */
package ch.njol.skript.util;

import java.util.TimeZone;

import org.jetbrains.annotations.Nullable;

import ch.njol.skript.SkriptConfig;
import ch.njol.yggdrasil.YggdrasilSerializable;

/**
 * @author Peter Güttinger
 */
public class Date implements Comparable<Date>, YggdrasilSerializable {
	
	/**
	 * Timestamp. Should always be in computer time/UTC/GMT+0.
	 */
	private long timestamp;
	
	public Date() {
		this(System.currentTimeMillis());
	}
	
	public Date(long timestamp) {
		this.timestamp = timestamp;
	}
	
	public Date(long timestamp, TimeZone zone) {
		final long offset = zone.getOffset(timestamp);
		this.timestamp = timestamp - offset;
	}
	
	/**
	 * Get a new Date with the current time
	 *
	 * @return New date with the current time
	 */
	public static Date now() {
		return new Date(System.currentTimeMillis());
	}
	
	public Timespan difference(Date other) {
		return new Timespan(Math.abs(timestamp - other.timestamp));
	}
	
	@Override
	public int compareTo(@Nullable Date other) {
		final long d = other == null ? timestamp : timestamp - other.timestamp;
		return d < 0 ? -1 : d > 0 ? 1 : 0;
	}

	/**
	 * Helper method to check if this date is before another date.
	 * See {@link #compareTo(Date)}.
	 *
	 * @param other The date to compare against
	 * @return {@code true} if this date is before the other date, {@code false} otherwise
	 */
	public boolean isBefore(Date other) {
		return compareTo(other) < 0;
	}

	/**
	 * Helper method to check if this date is after another date.
	 * See {@link #compareTo(Date)}.
	 *
	 * @param other The date to compare against
	 * @return {@code true} if this date is after the other date, {@code false} otherwise
	 */
	public boolean isAfter(Date other) {
		return compareTo(other) > 0;
	}
	
	@Override
	public String toString() {
		return SkriptConfig.formatDate(timestamp);
	}
	
	/**
	 * Get the timestamp of this date
	 *
	 * @return The timestamp in milliseconds
	 */
	public long getTimestamp() {
		return timestamp;
	}
	
	/**
	 * Add a {@link Timespan} to this date
	 *
	 * @param span Timespan to add
	 */
	public void add(Timespan span) {
		timestamp += span.getMilliSeconds();
	}
	
	/**
	 * Subtract a {@link Timespan} from this date
	 *
	 * @param span Timespan to subtract
	 */
	public void subtract(Timespan span) {
		timestamp -= span.getMilliSeconds();
	}
	
	/**
	 * Get a new instance of this Date with the added timespan
	 *
	 * @param span Timespan to add to this Date
	 * @return New Date with the added timespan
	 */
	public Date plus(Timespan span) {
		return new Date(timestamp + span.getMilliSeconds());
	}
	
	/**
	 * Get a new instance of this Date with the subtracted timespan
	 *
	 * @param span Timespan to subtract from this Date
	 * @return New Date with the subtracted timespan
	 */
	public Date minus(Timespan span) {
		return new Date(timestamp - span.getMilliSeconds());
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (int) (timestamp ^ (timestamp >>> 32));
		return result;
	}
	
	@Override
	public boolean equals(@Nullable Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (!(obj instanceof Date))
			return false;
		final Date other = (Date) obj;
		return timestamp == other.timestamp;
	}
	
}
