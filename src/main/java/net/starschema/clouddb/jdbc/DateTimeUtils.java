package net.starschema.clouddb.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoField;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.TimeZone;

class DateTimeUtils {

  private static final ZoneId UTC_ZONE = ZoneId.of("UTC");
  private static final Calendar DEFAULT_CALENDAR = new GregorianCalendar(TimeZone.getDefault());

  /** Formatter used to parse DATETIME literals. */
  private static final DateTimeFormatter DATETIME_FORMATTER =
      new DateTimeFormatterBuilder()
          // The date part is always YYYY-MM-DD.
          .appendValue(ChronoField.YEAR, 4)
          .appendLiteral('-')
          .appendValue(ChronoField.MONTH_OF_YEAR, 2)
          .appendLiteral('-')
          .appendValue(ChronoField.DAY_OF_MONTH, 2)
          // Accept either a literal 'T' or a space to separate the date from the time.
          // Make the space optional but pad with 'T' if it's omitted, so that parsing accepts both,
          // but formatting defaults to using the space.
          .padNext(1, 'T')
          .optionalStart()
          .appendLiteral(' ')
          .optionalEnd()
          // The whole-second time part is always HH:MM:SS.
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          // BigQuery has optional microsecond precision.
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 6, true)
          .optionalEnd()
          .toFormatter(Locale.ROOT);

  /** Formatter used to parse TIME literals. */
  private static final DateTimeFormatter TIME_FORMATTER =
      new DateTimeFormatterBuilder()
          // The whole-second time part is always HH:MM:SS.
          .appendValue(ChronoField.HOUR_OF_DAY, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.MINUTE_OF_HOUR, 2)
          .appendLiteral(':')
          .appendValue(ChronoField.SECOND_OF_MINUTE, 2)
          // BigQuery only has microsecond precision, but this parser supports up to nanosecond
          // precision after the decimal point.
          .optionalStart()
          .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
          .optionalEnd()
          .toFormatter(Locale.ROOT);

  private static final LocalDate TIME_EPOCH = LocalDate.of(1970, 1, 1);

  /** Parse a BigQuery DATETIME represented as a String. */
  static Timestamp parseDateTime(String value, Calendar cal) throws SQLException {
    if (cal == null) {
      cal = DEFAULT_CALENDAR;
    }
    try {
      // BigQuery DATETIME has a string representation that looks like e.g. "2010-10-26 02:49:35".
      LocalDateTime localDateTime = LocalDateTime.parse(value, DATETIME_FORMATTER);
      // In order to represent it as a [java.sql.Timestamp], which is defined by an instant,
      // we must subtract the offset from the calendar, so that the [toString] method of the
      // resulting [Timestamp] has the correct result. Since time zones can have different offsets
      // at different times of year (e.g. due to Daylight Saving), first calculate the instant
      // assuming no offset, and use that to calculate the offset.
      long utcInstant = localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(cal.getTimeZone().getOffset(utcInstant) / 1000);
      Instant instant = localDateTime.atOffset(offset).toInstant();
      Timestamp timestamp = new Timestamp(instant.toEpochMilli());
      timestamp.setNanos(instant.getNano());
      return timestamp;
    } catch (DateTimeParseException e) {
      throw new BQSQLException(e);
    }
  }

  /** Parse a BigQuery TIMESTAMP literal, represented as the number of seconds since epoch. */
  static Timestamp parseTimestamp(String value) throws SQLException {
    try {
      // BigQuery TIMESTAMP has a string representation that looks like e.g. "1.288061375E9"
      // for 2010-10-26 02:49:35 UTC.
      // It is the (possibly fractional) number of seconds since the epoch.
      BigDecimal secondsSinceEpoch = new BigDecimal(value);
      // https://stackoverflow.com/questions/5839411/java-sql-timestamp-way-of-storing-nanoseconds
      // In order to support sub-millisecond precision, we need to first initialize the timestamp
      // with the correct number of whole seconds (expressed in milliseconds),
      // then set the nanosecond value, which overrides the initial milliseconds.
      long wholeSeconds = secondsSinceEpoch.longValue();
      Timestamp timestamp = new Timestamp(wholeSeconds * 1000L);
      int nanoSeconds = secondsSinceEpoch.remainder(BigDecimal.ONE).movePointRight(9).intValue();
      timestamp.setNanos(nanoSeconds);
      return timestamp;
    } catch (NumberFormatException e) {
      throw new BQSQLException(e);
    }
  }

  static String formatTimestamp(String rawString) throws SQLException {
    Timestamp timestamp = parseTimestamp(rawString);
    return DATETIME_FORMATTER.format(OffsetDateTime.ofInstant(timestamp.toInstant(), UTC_ZONE))
        + " UTC";
  }

  static Date parseDate(String value, Calendar cal) throws SQLException {
    // Dates in BigQuery come back in the YYYY-MM-DD format
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
    try {
      java.util.Date date = sdf.parse(value);
      return new java.sql.Date(date.getTime());
    } catch (java.text.ParseException e) {
      throw new BQSQLException(e);
    }
  }

  static Time parseTime(String value, Calendar cal) throws SQLException {
    if (cal == null) {
      cal = DEFAULT_CALENDAR;
    }
    try {
      // BigQuery DATETIME has a string representation that looks like e.g. "2010-10-26 02:49:35".
      LocalTime localTime = LocalTime.parse(value, TIME_FORMATTER);
      // In order to represent it as a [java.sql.Time], which is defined by an instant,
      // we must subtract the offset from the calendar, so that the [toString] method of the
      // resulting [Time] has the correct result. Since time values do not have date components,
      // assume the standard offset should be used.
      ZoneOffset offset = ZoneOffset.ofTotalSeconds(cal.getTimeZone().getRawOffset() / 1000);
      Instant instant = localTime.atDate(TIME_EPOCH).atOffset(offset).toInstant();
      return new Time(instant.toEpochMilli());
    } catch (DateTimeParseException e) {
      throw new BQSQLException(e);
    }
  }
}
