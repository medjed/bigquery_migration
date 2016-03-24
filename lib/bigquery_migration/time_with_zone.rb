require 'tzinfo'

class BigqueryMigration
  class TimeWithZone
    # [+-]HH:MM, [+-]HHMM, [+-]HH
    NUMERIC_PATTERN = %r{\A[+-]\d\d(:?\d\d)?\z}

    # Region/Zone, Region/Zone/Zone
    NAME_PATTERN = %r{\A[^/]+/[^/]+(/[^/]+)?\z}

    class << self
      def time_with_zone(time, timezone)
        time.localtime(zone_offset(timezone))
      end

      def strptime_with_zone(date, format, timezone)
        time = Time.strptime(date, format)
        _utc_offset = time.utc_offset
        _zone_offset = zone_offset(timezone)
        time.localtime(_zone_offset) + _utc_offset - _zone_offset
      end

      private
      def zone_offset(timezone)
        if NUMERIC_PATTERN === timezone
          Time.zone_offset(timezone)
        elsif NAME_PATTERN === timezone
          tz = TZInfo::Timezone.get(timezone)
          tz.current_period.utc_total_offset
        elsif "UTC" == timezone # special treatment
          0
        else
          raise ArgumentError, "timezone format is invalid: #{timezone}"
        end
      end
    end
  end
end
