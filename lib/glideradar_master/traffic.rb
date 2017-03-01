#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

module GlideradarMaster

class Traffic
  class DataError < StandardError
  end

  attr_accessor :type
  attr_accessor :aircraft_id
  attr_accessor :flarm_identifier_type
  attr_accessor :flarm_identifier
  attr_accessor :aircraft_type_id
  attr_accessor :race_registration
  attr_accessor :registration

  attr_accessor :fn_owner_name
  attr_accessor :fn_home_airport
  attr_accessor :fn_type_name
  attr_accessor :fn_common_radio_frequency

  attr_accessor :timestamp

  attr_accessor :src
  attr_accessor :srcs

  attr_accessor :lat
  attr_accessor :lng
  attr_accessor :alt
  attr_accessor :cog
  attr_accessor :sog
  attr_accessor :tr
  attr_accessor :cr

  attr_reader :reception_state
  attr_reader :flying_state
  attr_reader :tow_state

  attr_reader :now

  protected
  attr_reader :log
  public

  FLARM_TYPES = {
    0x0 => 'unknown',
    0x1 => 'glider',
    0x2 => 'tow plane',
    0x3 => 'helicopter',
    0x4 => 'parachute',
    0x5 => 'drop plane',
    0x6 => 'fixed hang-glider',
    0x7 => 'soft para-glider',
    0x8 => 'powered aircraft',
    0x9 => 'jet aircraft',
    0xA => 'UFO',
    0xB => 'balloon',
    0xC => 'blimp, zeppelin',
    0xD => 'UAV',
    0xF => 'static'
  }.freeze

  TOWPLANE_TYPE = 0x2

  def initialize(now:, flarm_identifier_type:, flarm_identifier:, airfields:, data:, pg:, log:, event_cb:)
    @now = now
    @timestamp = now

    @type = data[:type]
    @flarm_identifier_type = flarm_identifier_type
    @flarm_identifier = flarm_identifier
    @airfields = airfields

    @pg = pg

    @log = log
    @event_cb = event_cb

    ####

    @elabuf = []
    @srcs = []

    @reception_state = :unknown
    @flying_state = :unknown
    @tow_state = :unknown

    @pg.transaction do
      res = @pg.exec_params("SELECT * FROM acao_aircrafts WHERE #{flarm_identifier_type}_identifier=$1", [ flarm_identifier ])

      if res.ntuples > 0
        row = res.first
        @aircraft_id = row['id']
        @aircraft_type_id = row['aircraft_type_id']
        @race_registration = row['race_registration']
        @registration = row['registration']
        @fn_owner_name = row['fn_owner_name']
        @fn_home_airport = row['fn_home_airport']
        @fn_type_name = row['fn_type_name']
        @fn_common_radio_frequency = row['fn_common_radio_frequency']
      else
        res = @pg.exec_params("INSERT INTO acao_aircrafts (#{flarm_identifier_type}_identifier) VALUES ($1) RETURNING id", [ flarm_identifier ])
        @aircraft_id = res[0]['id']
      end
    end

    event(:TRAFFIC_NEW, "New traffic, type (#{@type})=#{FLARM_TYPES[@type]}",
      aircraft_info: aircraft_info,
    )

    update(data: data)
  end

  def check_timetable_entry
    res = @pg.exec_params(
            "SELECT id FROM acao_timetable_entries WHERE aircraft_id=$1 " +
              "AND (takeoff_at IS NULL OR takeoff_at < $2) " +
              "AND (landing_at IS NULL OR landing_at > $2)", [ @aircraft_id, @now ])
    if res.ntuples > 0
      @timetable_entry_id = res[0]['id']
    else
      res = @pg.exec_params("INSERT INTO acao_timetable_entries (aircraft_id) VALUES ($1) RETURNING id", [ @aircraft_id ])
      @timetable_entry_id = res[0]['id']
    end
  end

  def update(data:)

log.debug "src=%6s now=%30s ts=%25s rcv_ts=%25s now-ts=%-5.1f" % [ data[:src], @now, data[:ts], data[:rcv_ts], @now - data[:ts] ]

    if data[:ts] > @now + 5.seconds
      log.error "Received updates from the future?? ts=#{data[:ts]} now=#{@now} diff=#{@now - data[:ts]}"
      return
    end

    @elabuf << data

    case @reception_state
    when :unknown
      change_reception_state(:alive)
    when :lost
      change_reception_state(:alive)
      alive!(since: data[:ts])
    when :alive
    end
  end

  def clock_event(now)
    if now < @now
      log.warn "Traffic #{self}: Non-monotonic timestamp in update_time (#{now} < #{@now})"
      raise DataError
    end

    @now = now

    check_alive!

    data_to_pick = @elabuf.select { |x| x[:ts] < @now - 1.second }

    if data_to_pick.any?
      data_to_pick.sort! { |a,b| a[:ts] <=> b[:ts] }

      @elabuf -= data_to_pick

      @srcs = data_to_pick.map { |x| x[:src] }.uniq
      src = data_to_pick.last

      @timestamp = src[:ts]
      @lat = src[:lat]
      @lng = src[:lng]
      @alt = src[:alt]
      @cog = src[:cog]
      @sog = src[:sog]
      @tr = src[:tr]
      @cr = src[:cr]
    end

    return if !valid?

    case @flying_state
    when :unknown
      if on_land?
        change_flying_state(:on_land)
        on_land!
      else
        change_flying_state(:flying)
        flying!
      end

    when :flying
      if on_land?
        @maybe_landing_since = @timestamp
        @maybe_landing_lat = @lat
        @maybe_landing_lng = @lng
        @maybe_landing_alt = @alt
        change_flying_state(:maybe_landing)
      end

    when :maybe_landing
      if on_land? && @timestamp - @maybe_landing_since > 5
        change_flying_state(:on_land)

        airfield = lookup_airfield(lat: @maybe_landing_lat, lng: @maybe_landing_lng, alt: @maybe_landing_alt)

        landed!(timestamp: @maybe_landing_since, lat: @maybe_landing_lat, lng: @maybe_landing_lng, alt: @maybe_landing_alt, airfield: airfield)
      end

    when :on_land
      if @sog > 15
        @maybe_taking_off_since = @timestamp
        @maybe_taking_off_lat = @lat
        @maybe_taking_off_lng = @lng
        @maybe_taking_off_alt = @alt
        change_flying_state(:maybe_taking_off)
      end

    when :maybe_taking_off
      if @sog > 15
        if @alt - @maybe_taking_off_alt > 30
          change_flying_state(:flying)

          airfield = lookup_airfield(lat: @maybe_taking_off_lat, lng: @maybe_taking_off_lng, alt: @maybe_taking_off_alt)

          takeoff!(timestamp: @timestamp, lat: @maybe_taking_off_lat, lng: @maybe_taking_off_lng, alt: @maybe_taking_off_alt, airfield: airfield)
        end
      else
        change_flying_state(:on_land)
      end
    end
  end

  def lookup_airfield(lat:, lng:, alt:)
    (icao_code, airfield) = @airfields.find do |airfield_icao, airfield|
      dist(lat1: lat, lng1: lng, lat2: airfield[:lat], lng2: airfield[:lng]) < airfield[:radius] &&
        (alt - airfield[:alt]).abs < 50
    end

    airfield
  end

  def dist(lat1:, lng1:, lat2:, lng2:)
    Math.sqrt( ((lat2 - lat1) * 1854 * 60)**2 +
               ((lng2 - lng1) * 1854 * 60 * Math.cos(lat1 / 180 * Math::PI))**2 )
  end

  def check_towed(other_tra)
    # Give height difference 3x the weight

    return if !alt || !lat || !lng || !other_tra.alt || !other_tra.lat || !other_tra.lng

    pseudist = Math.sqrt(((other_tra.alt - alt) * 3)**2 +
                        ((other_tra.lat - lat) * 1854 * 60)**2 +
                        ((other_tra.lng - lng) * 1854 * 60 * Math.cos(lat / 180 * Math::PI))**2 )

    case @tow_state
    when :unknown, :tow_released
      if @flying_state == :flying && other_tra.flying_state == :flying && other_tra.type == TOWPLANE_TYPE &&  pseudist < 250

        if @tow_state == :unknown
          change_tow_state(:maybe)
        else
          change_tow_state(:maybe_after_towing)
        end

        @tow_plane = other_tra
        @tow_cum_distance = pseudist
        @tow_samples = 1
        @tow_since = @timestamp
      end

    when :maybe, :maybe_after_towing
      if other_tra == @tow_plane
        @tow_cum_distance += pseudist
        @tow_samples += 1

        if @tow_samples > 10
  log.warn "TOWCUM #{@tow_cum_distance}"
          if @tow_cum_distance < 2000
            if @tow_state == :maybe_after_towing
              log.err "Tow detected in tow_released. Missed landing?"
              event(:TOW_ANOMALY, 'tow detected in tow_released. Missed landing?')
            end

            @pg.transaction do
              check_timetable_entry
              @tow_plane.check_timetable_entry

              res = @pg.exec_params("UPDATE acao_timetable_entries SET towed_by_id=$2  WHERE id=$1",
                       [ @timetable_entry_id, @tow_plane.aircraft_id ])
            end

            change_tow_state(:towing)
            event(:TOW_STARTED, "tow started tow_plane=#{@tow_plane}", tow_plane: @tow_plane)
          else
            change_tow_state(:unknown)
            @tow_plane = nil
          end
        end
      end

    when :towing
      if other_tra == @tow_plane && pseudist > 750

        @pg.transaction do
          check_timetable_entry

          res = @pg.exec_params("UPDATE acao_timetable_entries SET tow_duration=$2, tow_height=$3  WHERE id=$1",
                   [ @timetable_entry_id, @timestamp - @tow_since, @alt ])
        end

        change_tow_state(:tow_released)
        event(:TOW_RELEASED, 'tow released', tow_plane: @tow_plane, duration: @timestamp - @tow_since)
      end
    end
  end

  def remove!
    event(:TRAFFIC_REMOVED, "removed")
  end

  protected

  def check_alive!
    case @reception_state
    when :unknown, :lost
    when :alive
      if @elabuf.size == 0 && @timestamp && @now - @timestamp > 10.seconds
        change_reception_state(:lost)
        lost!
      end
    end
  end

  def takeoff!(timestamp:, lat:, lng:, alt:, airfield:)
    log.warn "Takeoff detected: ts=#{timestamp}, lat=#{lat}, lng=#{lng}, alt=#{alt} airfield=#{airfield}"

    @pg.transaction do
      check_timetable_entry

      res = @pg.exec_params("INSERT INTO core_locations (lat, lng, alt) VALUES ($1, $2, $3) RETURNING id", [ lat, lng, alt ])
      location_id = res[0]['id']

      res = @pg.exec_params("UPDATE acao_timetable_entries SET takeoff_at=$1, takeoff_location_id=$2, takeoff_airfield_id=$3 WHERE id=$4",
               [ timestamp, location_id, airfield ? airfield[:id] : nil, @timetable_entry_id ])
    end

    event(:TAKEOFF, 'Takeoff')
    flying!
  end

  def landed!(timestamp:, lat:, lng:, alt:, airfield:)
    log.warn "Landing detected: ts=#{timestamp}, lat=#{lat}, lng=#{lng}, alt=#{alt} airfield=#{airfield}"

    @pg.transaction do
      check_timetable_entry

      res = @pg.exec_params("INSERT INTO core_locations (lat, lng, alt) VALUES ($1, $2, $3) RETURNING id", [ lat, lng, alt ])
      location_id = res[0]['id']

      res = @pg.exec_params("UPDATE acao_timetable_entries SET landing_at=$1, landing_location_id=$2, landing_airfield_id=$3 WHERE id=$4",
               [ timestamp, location_id, airfield ? airfield[:id] : nil, @timetable_entry_id ])
    end

    event(:LAND, 'Landed')
    on_land!
  end

  def on_land!
    case @tow_state
    when :towing
      log.err "Towplane landed while towing?!"
      event(:TOW_ANOMALY, 'Landed while towing?!')
      change_tow_state(:unknown)
    when :maybe
      change_tow_state(:unknown)
    when :tow_released
      change_tow_state(:unknown)
    end
  end

  def flying!
    check_timetable_entry
  end

  def alive!(since:)
    event(:TRAFFIC_ALIVE, "Now alive", since: since)
  end

  def lost!
    event(:TRAFFIC_LOST, "Reception lost")
  end

  def on_land?
    @sog < 3 && @cr < 1.5
  end

  def change_reception_state(new_reception_state)
    log.debug "#{self} changed reception state from #{@reception_state} to #{new_reception_state}"

    @reception_state = new_reception_state

    @pg.exec_params("UPDATE acao_timetable_entries SET reception_state=$1 WHERE id=$2", [ @reception_state, @timetable_entry_id ])
  end

  def change_flying_state(new_flying_state)
    log.debug "#{self} changed flying state from #{@flying_state} to #{new_flying_state}"

    @flying_state = new_flying_state

    @pg.exec_params("UPDATE acao_timetable_entries SET flying_state=$1 WHERE id=$2", [ @flying_state, @timetable_entry_id ])
  end

  def change_tow_state(new_tow_state)
    log.debug "#{self} changed towing state from #{@tow_state} to #{new_tow_state}"

    @tow_state = new_tow_state

    @pg.exec_params("UPDATE acao_timetable_entries SET tow_state=$1 WHERE id=$2", [ @tow_state, @timetable_entry_id ])
  end

  def event(event_name, text, data = {})
    @event_cb.call(self, event_name, text, now, data.merge!(aircraft_id: @aircraft_id))
  end

  public

  def to_s
    "#{registration} (#{flarm_identifier})"
  end

  def valid?
    @timestamp && @lat && @lng && @alt
  end

  def flarm_combined_identifier
    "#{flarm_identifier_type}:#{flarm_identifier}"
  end

  def traffic_update
   {
    type: @type,
    flarm_id: @flarm_identifier,
    timestamp: @timestamp,

    src: @src,
    srcs: @srcs.join(','),

    lat: @lat,
    lng: @lng,
    alt: @alt,
    cog: @cog,
    sog: @sog,
    tr: @tr,
    cr: @cr,
   }
  end

  def aircraft_info
   {
    type: @type,
    aircraft_id: @aircraft_id,
    flarm_id: @flarm_identifier,

    aircraft_aircraft_type_id: @aircraft_aircraft_type_id,
    race_registration: @race_registration,
    registration: @registration,
    fn_owner_name: @fn_owner_name,
    fn_home_airport: @fn_home_airport,
    fn_type_name: @fn_type_name,
    fn_common_radio_frequency: @fn_common_radio_frequency,
   }
  end
end

end
