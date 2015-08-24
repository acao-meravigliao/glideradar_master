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
  attr_accessor :plane_id
  attr_accessor :flarm_id
  attr_accessor :flarm_code
  attr_accessor :icao_code

  attr_accessor :owner_name
  attr_accessor :home_airport
  attr_accessor :type_id
  attr_accessor :type_name
  attr_accessor :race_registration
  attr_accessor :registration
  attr_accessor :common_radio_frequency

  attr_accessor :last_update

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
  attr_reader :towing_state

  attr_reader :now

  protected
  attr_reader :log
  public

  def initialize(now:, plane_id:, flarm_id:, flarm_code:, icao_code:, data:, plane_data:, source:, log:, event_cb:)
    @srcs = {}

    @type = data[:type]
    @plane_id = plane_id
    @flarm_id = flarm_id
    @flarm_code = flarm_code
    @icao_code = icao_code

    @reception_state = :unknown
    @flying_state = :unknown
    @towing_state = :unknown

    @log = log
    @event_cb = event_cb

    @owner_name = plane_data[:owner_name]
    @home_airport = plane_data[:home_airport]
    @type_id = plane_data[:type_id]
    @type_name = plane_data[:type_name]
    @race_registration = plane_data[:race_registration]
    @registration = plane_data[:registration]
    @common_radio_frequency = plane_data[:common_radio_frequency]

    @now = now

    event(:TRAFFIC_NEW, "New traffic, type #{@type}",
      plane_info: plane_data,
    )

    update(data: data, source: source)
  end

  def update(data:, source:)
    since = @last_update
    @new_srcs ||= {}

    data[:ts] = Time.parse(data[:ts])

    @last_update = data[:ts]

    @new_srcs[source] = data

    case @reception_state
    when :unknown
      change_reception_state(:alive)
    when :lost
      change_reception_state(:alive)
      alive!(since: since)
    when :alive
    end
  end

  def updates_complete
    @srcs = @new_srcs
    @new_srcs = nil

    return if @srcs.count == 0

    src = @srcs.values.sort { |x| x[:ts].to_i }.last

    @lat = src[:lat]
    @lng = src[:lng]
    @alt = src[:alt]
    @cog = src[:cog]
    @sog = src[:sog]
    @tr = src[:tr]
    @cr = src[:cr]

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
        @maybe_on_land_since = @last_update
        change_flying_state(:maybe_on_land)
      end

    when :maybe_on_land
      if on_land? && @last_update - @maybe_on_land_since > 5
        change_flying_state(:on_land)
        landed!
      end

    when :on_land
      if @sog > 15
        @maybe_flying_since = @last_update
        @maybe_flying_alt = @alt
        change_flying_state(:maybe_flying)
      end

    when :maybe_flying
      if @sog > 15
        if @alt - @maybe_flying_alt > 30
          change_flying_state(:flying)
          takeoff!
        end
      else
        change_flying_state(:on_land)
      end
    end
  end

  def update_time(now)
    if now < @now
      log.warn "Traffic #{self}: Non-monotonic timestamp in update_time (#{now} < #{@now})"
      raise DataError
    end

    @now = now

    check_alive!
  end

  def check_towed(other_tra)
    # Give height difference 3x the weight

    return if !alt || !lat || !lng || !other_tra.alt || !other_tra.lat || !other_tra.lng

    pseudist = Math.sqrt(((other_tra.alt - alt) * 3)**2 +
                        ((other_tra.lat - lat) * 1854 * 60)**2 +
                        ((other_tra.lng - lng) * 1854 * 60 * Math.cos(lat / 180 * Math::PI))**2 )

    case @towing_state
    when :unknown, :tow_released
      if @flying_state == :flying && other_tra.flying_state == :flying && pseudist < 250

        if @towing_state == :unknown
          change_towing_state(:maybe)
        else
          change_towing_state(:maybe_after_towing)
        end

        @towing_glider = other_tra
        @towing_cum_distance = pseudist
        @towing_samples = 1
        @towing_since = @last_update
      end
    when :maybe, :maybe_after_towing
      if other_tra == @towing_glider
        @towing_cum_distance += pseudist
        @towing_samples += 1

        if @towing_samples > 10
  log.warn "TOWCUM #{@towing_cum_distance}"
          if @towing_cum_distance < 2000
            if @towing_state == :maybe_after_towing
              log.err "Tow detected in tow_released. Missed landing?"
              event(:TOW_ANOMALY, 'tow detected in tow_released. Missed landing?')
            end

            change_towing_state(:towing)
            event(:TOW_STARTED, "tow started with #{@towing_glider}", towing: @towing_glider)
          else
            change_towing_state(:unknown)
            @towing_glider = nil
          end
        end
      end

    when :towing
      if other_tra == @towing_glider && pseudist > 750
        change_towing_state(:tow_released)
        event(:TOW_RELEASED, 'tow released', towing: @towing_glider, duration: @last_update - @towing_since)
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
      if now - last_update > 10.seconds
        change_reception_state(:lost)
        lost!
      end
    end
  end

  def takeoff!
    event(:TAKEOFF, 'Takeoff')
    flying!
  end

  def landed!
    event(:LAND, 'Landed')
    on_land!
  end

  def on_land!
    case @towing_state
    when :towing
      log.err "Towplane landed while towing?!"
      event(:TOW_ANOMALY, 'Landed while towing?!')
      change_towing_state(:unknown)
    when :maybe
      change_towing_state(:unknown)
    when :tow_released
      change_towing_state(:unknown)
    end
  end

  def flying!
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
    log.debug "changed reception state from #{@reception_state} to #{new_reception_state}"
    @reception_state = new_reception_state
  end

  def change_flying_state(new_flying_state)
    log.debug "changed flying state from #{@flying_state} to #{new_flying_state}"
    @flying_state = new_flying_state
  end

  def change_towing_state(new_towing_state)
    log.debug "changed towing state from #{@towing_state} to #{new_towing_state}"
    @towing_state = new_towing_state
  end

  def event(event_name, text, data = {})
    @event_cb.call(self, event_name, text, now, data.merge!(plane_id: @plane_id))
  end

  public

  def to_s
    "#{registration} (#{flarm_code || icao_code})"
  end

  def valid?
    @last_update && @lat && @lng && @alt
  end

  def traffic_update
   {
    type: @type,
    plane_id: @plane_id,
    last_update: @last_update,

    lat: @lat,
    lng: @lng,
    alt: @alt,
    cog: @cog,
    sog: @sog,
    tr: @tr,
    cr: @cr,
   }
  end

  def plane_info
   {
    type: @type,
    plane_id: @plane_id,
    flarm_id: @flarm_id,
    flarm_code: @flarm_code,
    icao_code: @icao_code,

    owner_name: @owner_name,
    home_airport: @home_airport,
    type_id: @type_id,
    type_name: @type_name,
    race_registration: @race_registration,
    registration: @registration,
    common_radio_frequency: @common_radio_frequency,
   }
  end
end

end
