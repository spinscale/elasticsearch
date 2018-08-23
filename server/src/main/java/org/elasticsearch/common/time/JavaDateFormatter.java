/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.time;

import org.elasticsearch.ElasticsearchParseException;

import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.time.temporal.TemporalField;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

class JavaDateFormatter implements DateFormatter {

    private final String format;
    private final DateTimeFormatter printer;
    private final DateTimeFormatter[] parsers;

    JavaDateFormatter(String format, DateTimeFormatter printer, DateTimeFormatter... parsers) {
        long distinctZones = Arrays.stream(parsers).map(DateTimeFormatter::getZone).distinct().count();
        if (distinctZones > 1) {
            throw new IllegalArgumentException("formatters must have the same time zone");
        }
        Set<Locale> locales = new HashSet<>(parsers.length);
        for (DateTimeFormatter parser : parsers) {
            locales.add(parser.getLocale());
        }
        if (locales.size() > 1) {
            throw new IllegalArgumentException("formatters must have the same locale");
        }
        this.printer = printer;
        this.format = format;
        if (parsers.length == 0) {
            this.parsers = new DateTimeFormatter[]{printer};
        } else {
            this.parsers = parsers;
        }
    }

    @Override
    public TemporalAccessor parse(String input) {
        ElasticsearchParseException failure = null;
        for (int i = 0; i < parsers.length; i++) {
            try {
                return parsers[i].parse(input);
            } catch (DateTimeParseException e) {
                if (failure == null) {
                    failure = new ElasticsearchParseException("could not parse input [" + input +
                        "] with date formatter [" + format + "]");
                }
                failure.addSuppressed(e);
            }
        }

        // ensure that all parsers exceptions are returned instead of only the last one
        throw failure;
    }

    @Override
    public DateFormatter withZone(ZoneId zoneId) {
        // shortcurt to not create new objects unnecessarily
        if (zoneId.equals(parsers[0].getZone())) {
            return this;
        }

        final DateTimeFormatter[] parsersWithZone = new DateTimeFormatter[parsers.length];
        for (int i = 0; i < parsers.length; i++) {
            parsersWithZone[i] = parsers[i].withZone(zoneId);
        }

        return new JavaDateFormatter(format, printer.withZone(zoneId), parsersWithZone);
    }

    public String format(TemporalAccessor accessor) {
        return printer.format(accessor);
    }

    @Override
    public String pattern() {
        return format;
    }

    public DateFormatter parseDefaulting(Map<TemporalField, Long> fields) {
        final DateTimeFormatterBuilder parseDefaultingBuilder = new DateTimeFormatterBuilder().append(printer);
        fields.forEach(parseDefaultingBuilder::parseDefaulting);
        if (parsers.length == 1 && parsers[0].equals(printer)) {
            return new JavaDateFormatter(format, parseDefaultingBuilder.toFormatter(Locale.ROOT));
        } else {
            final DateTimeFormatter[] parsersWithDefaulting = new DateTimeFormatter[parsers.length];
            for (int i = 0; i < parsers.length; i++) {
                DateTimeFormatterBuilder builder = new DateTimeFormatterBuilder().append(parsers[i]);
                fields.forEach(builder::parseDefaulting);
                parsersWithDefaulting[i] = builder.toFormatter(Locale.ROOT);
            }
            return new JavaDateFormatter(format, parseDefaultingBuilder.toFormatter(Locale.ROOT), parsersWithDefaulting);
        }
    }

    @Override
    public int hashCode() {
        // TODO add locale, remove printer, parsers?
        return Objects.hash(printer, format, Arrays.hashCode(parsers));
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(this.getClass()) == false) {
            return false;
        }
        JavaDateFormatter other = (JavaDateFormatter) obj;

        // TODO add locale, remove printer parsers?
        return Objects.equals(format, other.format) &&
               Objects.equals(printer, other.printer) &&
               Arrays.equals(parsers, other.parsers);
    }

    @Override
    public String toString() {
        // TODO add locale
        return String.format(Locale.ROOT, "format[%s]", format);
    }
}
