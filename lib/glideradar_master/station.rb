#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

module GlideradarMaster

class Station
  class DataError < StandardError
  end

  attr_accessor :flarm_code
  attr_accessor :name
  attr_accessor :last_update

  attr_reader :reception_state

  attr_accessor :lat
  attr_accessor :lng
  attr_accessor :alt
  attr_accessor :cog
  attr_accessor :sog
  attr_accessor :gps_fix_qual
  attr_accessor :gps_sats
  attr_accessor :gps_fix_type
  attr_accessor :gps_pdop
  attr_accessor :gps_hdop
  attr_accessor :gps_vdop

  attr_accessor :now

  protected
  attr_reader :log

  public

  def initialize(now:, name:, data:, log:, event_cb:)
    @name = name
    @log = log
    @event_cb = event_cb

    @reception_state = :unknown

    @now = now

    event(:STATION_NEW, "New station found", sta_id: @name)

    update(data: data)
  end

  def update(data:)
    since = @last_update

    @last_update = Time.parse(data[:time])

    @lat = data[:lat]
    @lng = data[:lng]
    @alt = data[:alt]
    @cog = data[:cog]
    @sog = data[:sog]
    @gps_fix_qual = data[:gps_fix_qual]
    @gps_sats = data[:gps_sats]
    @gps_fix_type = data[:gps_fix_type]
    @gps_pdop = data[:gps_pdop]
    @gps_hdop = data[:gps_hdop]
    @gps_vdop = data[:gps_vdop]

    case @reception_state
    when :unknown
      change_reception_state(:alive)
    when :lost
      change_reception_state(:alive)
      alive!(since: since)
    when :alive
    end
  end

  def processed_representation
   {
    flarm_code: @flarm_code,
    name: @name,

    lat: @lat,
    lng: @lng,
    alt: @alt,
    cog: @cog,
    sog: @sog,
    gps_fix_qual: @gps_fix_qual,
    gps_sats: @gps_sats,
    gps_fix_type: @gps_fix_type,
    gps_pdop: @gps_pdop,
    gps_hdop: @gps_hdop,
    gps_vdop: @gps_vdop,

    last_update: @last_update,
   }
  end

  def clock_event(now)
    if @now && now < @now
      log.warn "Station #{self}: Non-monotonic timestamp in update_time (#{now} < #{@now})"
      raise DataError
    end

    @now = now

    check_alive!
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

  def alive!(since:)
    event(:STATION_ONLINE, "Now online", since: since)
  end

  def lost!
    event(:STATION_OFFLINE, "Went offline")
  end

  def change_reception_state(new_reception_state)
    log.debug "#{now}: changed reception state from #{@reception_state} to #{new_reception_state}"
    @reception_state = new_reception_state
  end

  def event(event_name, text, **data)
    @event_cb.call(sta: self, event: event_name, text: text, **data)
  end

  public

  def to_s
    name
  end
end

end
