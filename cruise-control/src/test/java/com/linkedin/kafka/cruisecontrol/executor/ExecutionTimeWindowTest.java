/*
 * Copyright 2026 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License"). See License in the project root for license information.
 */

package com.linkedin.kafka.cruisecontrol.executor;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class ExecutionTimeWindowTest {
  private static final ZoneId UTC = ZoneId.of("UTC");
  private static final ZoneId TEHRAN = ZoneId.of("Asia/Tehran");
  private static final ZoneId BERLIN = ZoneId.of("Europe/Berlin");

  private static long timeMs(ZoneId zoneId, int day, int hour, int minute) {
    return ZonedDateTime.of(2026, 9, day, hour, minute, 0, 0, zoneId).toInstant().toEpochMilli();
  }

  @Test
  public void testIsOpen() {
    ExecutionTimeWindow window = new ExecutionTimeWindow(7, 14, UTC);
    assertFalse(window.isOpen(timeMs(UTC, 28, 6, 59)));
    // Start hour is inclusive.
    assertTrue(window.isOpen(timeMs(UTC, 28, 7, 0)));
    assertTrue(window.isOpen(timeMs(UTC, 28, 13, 59)));
    // End hour is exclusive.
    assertFalse(window.isOpen(timeMs(UTC, 28, 14, 0)));
    assertFalse(window.isOpen(timeMs(UTC, 28, 23, 0)));
    assertFalse(window.isOpen(timeMs(UTC, 28, 0, 0)));
  }

  @Test
  public void testIsOpenRespectsTimeZone() {
    ExecutionTimeWindow window = new ExecutionTimeWindow(7, 14, TEHRAN);
    // 07:00 in Tehran (UTC+03:30) is 03:30 in UTC.
    assertTrue(window.isOpen(timeMs(TEHRAN, 28, 7, 0)));
    assertTrue(window.isOpen(timeMs(UTC, 28, 3, 30)));
    // 03:00 in UTC is 06:30 in Tehran, and 11:00 in UTC is 14:30 in Tehran -- both outside the window.
    assertFalse(window.isOpen(timeMs(UTC, 28, 3, 0)));
    assertFalse(window.isOpen(timeMs(UTC, 28, 11, 0)));
    // 07:00 in UTC is 10:30 in Tehran, which is within the window although 07:00 is its start hour in UTC.
    assertTrue(window.isOpen(timeMs(UTC, 28, 7, 0)));
  }

  @Test
  public void testIsOpenForWindowSpanningMidnight() {
    ExecutionTimeWindow window = new ExecutionTimeWindow(22, 6, UTC);
    assertFalse(window.isOpen(timeMs(UTC, 28, 21, 59)));
    assertTrue(window.isOpen(timeMs(UTC, 28, 22, 0)));
    assertTrue(window.isOpen(timeMs(UTC, 28, 23, 59)));
    assertTrue(window.isOpen(timeMs(UTC, 29, 0, 0)));
    assertTrue(window.isOpen(timeMs(UTC, 29, 5, 59)));
    assertFalse(window.isOpen(timeMs(UTC, 29, 6, 0)));
    assertFalse(window.isOpen(timeMs(UTC, 29, 12, 0)));
  }

  @Test
  public void testNextOpenTimeMs() {
    ExecutionTimeWindow window = new ExecutionTimeWindow(7, 14, UTC);
    // Open now.
    long now = timeMs(UTC, 28, 9, 15);
    assertEquals(now, window.nextOpenTimeMs(now));
    // Before the window opens on the same day.
    assertEquals(timeMs(UTC, 28, 7, 0), window.nextOpenTimeMs(timeMs(UTC, 28, 3, 20)));
    // After the window closes -- opens on the next day.
    assertEquals(timeMs(UTC, 29, 7, 0), window.nextOpenTimeMs(timeMs(UTC, 28, 14, 0)));
    assertEquals(timeMs(UTC, 29, 7, 0), window.nextOpenTimeMs(timeMs(UTC, 28, 23, 59)));

    ExecutionTimeWindow overnightWindow = new ExecutionTimeWindow(22, 6, UTC);
    assertEquals(timeMs(UTC, 28, 22, 0), overnightWindow.nextOpenTimeMs(timeMs(UTC, 28, 6, 0)));
  }

  @Test
  public void testNextOpenTimeMsAcrossDaylightSavingTimeChange() {
    // Daylight saving time in Europe/Berlin ends on 2026-10-25 at 03:00 (clocks go back to 02:00).
    ExecutionTimeWindow window = new ExecutionTimeWindow(7, 14, BERLIN);
    long expected = ZonedDateTime.of(2026, 10, 25, 7, 0, 0, 0, BERLIN).toInstant().toEpochMilli();
    long now = ZonedDateTime.of(2026, 10, 24, 20, 0, 0, 0, BERLIN).toInstant().toEpochMilli();
    assertEquals(expected, window.nextOpenTimeMs(now));
  }

  @Test
  public void testInvalidWindow() {
    assertThrows(IllegalArgumentException.class, () -> new ExecutionTimeWindow(-1, 14, UTC));
    assertThrows(IllegalArgumentException.class, () -> new ExecutionTimeWindow(7, 24, UTC));
    assertThrows(IllegalArgumentException.class, () -> new ExecutionTimeWindow(7, 7, UTC));
    assertThrows(NullPointerException.class, () -> new ExecutionTimeWindow(7, 14, null));
  }

  @Test
  public void testToString() {
    assertEquals("[07:00, 14:00) UTC", new ExecutionTimeWindow(7, 14, UTC).toString());
  }
}
