/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseClass;
import com.linkedin.kafka.cruisecontrol.servlet.response.JsonResponseField;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * A daily time-of-day window, in hours, within which the {@link Executor} is allowed to start new movement batches.
 *
 * <ul>
 *   <li>The window is {@code [startHour, endHour)} in the given time zone -- i.e. start hour is inclusive and end hour is exclusive.</li>
 *   <li>If {@code startHour > endHour}, the window wraps around midnight (e.g. {@code [22, 6)} is open from 22:00 to 06:00).</li>
 *   <li>The window only gates the start of new batches. Movements that are already in progress when the window closes are
 *   allowed to finish; they are never stopped or rolled back because of the window.</li>
 * </ul>
 */
@JsonResponseClass
public final class ExecutionTimeWindow {
  public static final int MIN_HOUR = 0;
  public static final int MAX_HOUR = 23;
  @JsonResponseField
  private static final String START_HOUR = "startHour";
  @JsonResponseField
  private static final String END_HOUR = "endHour";
  @JsonResponseField
  private static final String TIME_ZONE = "timeZone";
  private final int _startHour;
  private final int _endHour;
  private final ZoneId _zoneId;

  /**
   * @param startHour Hour of the day (inclusive, 0-23) at which the window opens.
   * @param endHour Hour of the day (exclusive, 0-23) at which the window closes.
   * @param zoneId Time zone in which the hours are interpreted.
   */
  public ExecutionTimeWindow(int startHour, int endHour, ZoneId zoneId) {
    validateHour(startHour, "Start");
    validateHour(endHour, "End");
    if (startHour == endHour) {
      throw new IllegalArgumentException(String.format("Execution time window start hour and end hour cannot be the same (%d).",
                                                       startHour));
    }
    _startHour = startHour;
    _endHour = endHour;
    _zoneId = Objects.requireNonNull(zoneId, "Time zone cannot be null.");
  }

  private static void validateHour(int hour, String name) {
    if (hour < MIN_HOUR || hour > MAX_HOUR) {
      throw new IllegalArgumentException(String.format("%s hour of execution time window must be in [%d, %d] (given: %d).",
                                                       name, MIN_HOUR, MAX_HOUR, hour));
    }
  }

  public int startHour() {
    return _startHour;
  }

  public int endHour() {
    return _endHour;
  }

  public ZoneId zoneId() {
    return _zoneId;
  }

  /**
   * @param timeMs Time in epoch milliseconds.
   * @return {@code true} if the window is open at the given time, {@code false} otherwise.
   */
  public boolean isOpen(long timeMs) {
    int hour = Instant.ofEpochMilli(timeMs).atZone(_zoneId).getHour();
    return _startHour < _endHour ? (hour >= _startHour && hour < _endHour) : (hour >= _startHour || hour < _endHour);
  }

  /**
   * @param timeMs Time in epoch milliseconds.
   * @return The given time if the window is open at that time, otherwise the earliest time after it at which the window opens.
   */
  public long nextOpenTimeMs(long timeMs) {
    if (isOpen(timeMs)) {
      return timeMs;
    }
    ZonedDateTime now = Instant.ofEpochMilli(timeMs).atZone(_zoneId);
    ZonedDateTime next = now.truncatedTo(ChronoUnit.DAYS).withHour(_startHour);
    if (!next.isAfter(now)) {
      next = now.plusDays(1).truncatedTo(ChronoUnit.DAYS).withHour(_startHour);
    }
    return next.toInstant().toEpochMilli();
  }

  /**
   * @param timeMs Time in epoch milliseconds.
   * @return Human-readable representation of the time at which the window opens next, in the window's time zone.
   */
  public String nextOpenTime(long timeMs) {
    return Instant.ofEpochMilli(nextOpenTimeMs(timeMs)).atZone(_zoneId).toOffsetDateTime().toString();
  }

  /**
   * @return An object that can be further used to encode into JSON.
   */
  public Map<String, Object> getJsonStructure() {
    Map<String, Object> jsonStructure = new HashMap<>();
    jsonStructure.put(START_HOUR, _startHour);
    jsonStructure.put(END_HOUR, _endHour);
    jsonStructure.put(TIME_ZONE, _zoneId.getId());
    return jsonStructure;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ExecutionTimeWindow that = (ExecutionTimeWindow) o;
    return _startHour == that._startHour && _endHour == that._endHour && _zoneId.equals(that._zoneId);
  }

  @Override
  public int hashCode() {
    return Objects.hash(_startHour, _endHour, _zoneId);
  }

  @Override
  public String toString() {
    return String.format("[%02d:00, %02d:00) %s", _startHour, _endHour, _zoneId.getId());
  }
}
