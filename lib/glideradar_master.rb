#!/usr/bin/env ruby
#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'ygg/app/line_buffer'

require 'glideradar_master/version'
require 'glideradar_master/task'

require 'securerandom'
require 'time'

require 'pg'

module GlideradarMaster

class Station
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

  protected
  attr_reader :log

  public

  def initialize(name:, data:, log:, event_cb:)
    @name = name
    @log = log
    @event_cb = event_cb

    @reception_state = :unknown

    update(data)
  end

  def update(data)
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
    when :unknown, :lost
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

  def check_alive(now)
    case @reception_state
    when :unknown, :lost
    when :alive
      if now - last_update > 10.seconds
        change_reception_state(:lost)
        lost!
      end
    end
  end

  protected

  def alive!(since:)
    event(:STATION_ONLINE, "Now online", since: since)
  end

  def lost!
    event(:STATION_OFFLINE, "Went offline")
  end

  def change_reception_state(new_reception_state)
    log.debug "changed reception state from #{@reception_state} to #{new_reception_state}"
    @reception_state = new_reception_state
  end

  def event(event_name, text, data = {})
    @event_cb.call(self, event_name, text, data)
  end

  public

  def to_s
    name
  end
end

class Obj
  class DataError < StandardError
  end

  attr_accessor :type
  attr_accessor :plane_id
  attr_accessor :registration
  attr_accessor :flarm_code
  attr_accessor :icao_code
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

  protected
  attr_reader :log
  public

  def initialize(obj_id:, data:, source:, log:, event_cb:)
    @srcs = {}

    @type = data[:type]

    if obj_id =~ /^flarm:(.*)/
      @flarm_code = $1
    elsif obj_id =~ /^icao:(.*)/
      obj.icao_code = $1
    end

    @reception_state = :unknown
    @flying_state = :unknown
    @towing_state = :unknown

    @log = log
    @event_cb = event_cb

    update(data: data, source: source)
  end

  def update(data:, source:)
    if !data[:ts]
      log.warn "Spurious data received: no ts in update"
      raise DataError
    end

    since = @last_update

    @last_update = Time.parse(data[:ts])
    data[:last_update] = data[:ts]

    @srcs[source] = data

    case @reception_state
    when :unknown, :lost
      change_reception_state(:alive)
      alive!(since: since)
    when :alive
    end
  end

  def updates_complete
    return if @srcs.count == 0

    src = @srcs.first[1]

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
        end
      else
        change_flying_state(:on_land)
        takeoff!
      end
    end
  end

  def check_alive(now)
    case @reception_state
    when :unknown, :lost
    when :alive
      if now - last_update > 10.seconds
        change_reception_state(:lost)
        lost!
      end
    end
  end

  def check_towed(other_obj)
    # Give height difference 3x the weight

    return if !alt || !lat || !lng || !other_obj.alt || !other_obj.lat || !other_obj.lng

    pseudist = Math.sqrt(((other_obj.alt - alt) * 3)**2 +
                        ((other_obj.lat - lat) * 1854 * 60)**2 +
                        ((other_obj.lng - lng) * 1854 * 60 * Math.cos(lat / 180 * Math::PI))**2 )

    case @towing_state
    when :unknown, :tow_released
      if @flying_state == :flying && other_obj.flying_state == :flying && pseudist < 250

        if @towing_state == :unknown
          change_towing_state(:maybe)
        else
          change_towing_state(:maybe_after_towing)
        end

        @towing_glider = other_obj
        @towing_cum_distance = pseudist
        @towing_samples = 1
        @towing_since = @last_update
      end
    when :maybe, :maybe_after_towing
      if other_obj == @towing_glider
        @towing_cum_distance += pseudist
        @towing_samples += 1

        if @towing_samples > 10
  log.warn "TOWCUM #{@towing_cum_distance}"
          if @towing_cum_distance < 2000
            if @towing_state == :maybe_after_towing
              log.err "Tow detected in tow_released. Missed landing?"
              event(:TOW_ANOMALY, 'Tow detected in tow_released. Missed landing?')
            end

            change_towing_state(:towing)
            event(:TOW_STARTED, 'Tow started', towing: @towing_glider)
          else
            change_towing_state(:unknown)
            @towing_glider = nil
          end
        end
      end

    when :towing
      if other_obj == @towing_glider && pseudist > 750
        change_towing_state(:tow_released)
        @tow_ended_cb.call(:TOW_RELEASED, 'Tow released', towing: @towing_glider, duration: @last_update - @towing_since)
      end
    end
  end


  protected

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
    event(:OBJECT_ALIVE, "Now alive", since: since)
  end

  def lost!
    event(:OBJECT_LOST, "Reception lost")
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
    @event_cb.call(self, event_name, text, data)
  end

  public

  def to_s
    "#{registration} (#{flarm_code || icao_code})"
  end

  def valid?
    @last_update && @lat && @lng && @alt
  end

  def processed_representation
   {
    type: @type,
    plane_id: @plane_id,
    registration: @registration,
    flarm_code: @flarm_code,
    icao_code: @icao_code,
    last_update: @last_update,

    lat: @lat,
    lng: @lng,
    alt: @alt,
    cog: @cog,
    sog: @sog,
    tr: @tr,
    cr: @cr,

    flying_state: @flying_state,
   }
  end
end

class Recorder
  include AM::Actor

  class MsgRecord < AM::Msg
    attr_accessor :objects
  end
  class MsgRecordOk < AM::Msg ; end
  class MsgRecordFailure < AM::Msg ; end

  def initialize(db_config:, **args)
    super(**args)

    @db_config = db_config
  end

  def actor_boot
    @pg = PG::Connection.open(@db_config)
    @ins_statement = @pg.prepare('ins',
      'INSERT INTO track_entries (at, plane_id, lat, lng, alt, cog, sog, tr, cr) ' +
      'VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)')
  end

  def handle(message)
    case message
    when MsgRecord
      @pg.transaction do
        message.objects.each do |obj|
          @pg.exec_prepared('ins', [ obj[:at], obj[:plane_id], obj[:lat], obj[:lng], obj[:alt], obj[:cog], obj[:sog], obj[:tr], obj[:cr] ])
        end
      end

      reply message, MsgRecordOk.new
    else
      super
    end
  end
end


class App < Ygg::Agent::Base
  self.app_name = 'glideradar_master'
  self.app_version = VERSION
  self.task_class = Task

  def prepare_default_config
    app_config_files << File.join(File.dirname(__FILE__), '..', 'config', 'glideradar_master.conf')
    app_config_files << '/etc/yggdra/glideradar_master.conf'
  end


  def agent_boot
    @pg = PG::Connection.open(mycfg.db.to_h)

    @recorder = actor_supervise_new(Recorder, {
      actor_name: :recorder,
      db_config: mycfg.db.to_h,
      monitored_by: self,
    })

    @msg_exchange = @amqp.ask(AM::AMQP::MsgDeclareExchange.new(
      name: mycfg.exchange,
      type: :topic,
      options: {
        durable: true,
        auto_delete: false,
      }
    )).value.exchange_id

    @msg_queue = @amqp.ask(AM::AMQP::MsgDeclareQueue.new(
      name: mycfg.queue,
      options: {
        durable: true,
        auto_delete: false,
        :'x-message-ttl' => (3 * 86400 * 1000),
      }
    )).value.queue_id

    @amqp.ask(AM::AMQP::MsgBind.new(queue_id: @msg_queue, exchange_id: @msg_exchange, options: { routing_key: '#' })).value

    @msg_consumer = @amqp.ask(AM::AMQP::MsgSubscribe.new(
      queue_id: @msg_queue,
      send_to: self.actor_ref,
      manual_ack: true)).value.consumer_tag

    @processed_exchange = @amqp.ask(AM::AMQP::MsgDeclareExchange.new(
      name: mycfg.processed_traffic_exchange,
      type: :topic,
      options: {
        durable: true,
        auto_delete: false,
      }
    )).value.exchange_id

    @pending_recs = {}
    @pending_acks = {}

    @objects = {}
    @updated_objects = {}
    @towplanes = {}
    @stations = {}

    @clock_source = nil
    @clock_timeout = delay(5.seconds) do
      event(:LOST_CLOCK, "Lost clock #{@clock_source}", clock_source: @clock_source)
      @clock_source = nil
      @clock_internal.start!
    end

    @clock_internal = actor_set_timer(delay: 1.second, interval: 1.second, start: false) do
      if !@clock_source
        @time = Time.now
        clock_event
      end
    end
  end

  def handle(message)
    case message
    when AM::AMQP::MsgDelivery

      if message.delivery_info.consumer_tag == @msg_consumer
        case message.properties[:type]
        when 'STATION_UPDATE'
          rcv_station_update(message.payload.deep_symbolize_keys!)
          @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: message.delivery_info.delivery_tag)
        when 'TRAFFIC_UPDATE'
          rcv_traffic_update(message.payload.deep_symbolize_keys!,
                             delivery_tag: message.delivery_info.delivery_tag)
        else
          @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: message.delivery_info.delivery_tag)
        end
      else
        super
      end
    when Recorder::MsgRecordOk, Recorder::MsgRecordFailure
      delivery_tags = @pending_recs[message.in_reply_to.object_id]
      if delivery_tags
        delivery_tags.each do |delivery_tag|
          @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: delivery_tag)
        end
      else
        log.err "Unable to find message to acknowledge!"
      end
    else
      super
    end
  end

  def periodic_cleanup
    objects_to_remove = []

    @objects.each do |obj_id, obj|
      obj.check_alive(@time)

      if @time - obj.last_update > 120.seconds
        event(:OBJECT_REMOVED, "Obj #{obj} removed", obj_id: obj_id, obj: obj.processed_representation)
        objects_to_remove << obj_id
      end
    end

    objects_to_remove.each do |obj_id|
      @objects.delete obj_id
      @towplanes.delete obj_id
    end

    @stations.each do |sta_id, sta|
      sta.check_alive(@time)
    end
  end

  def rcv_station_update(message)
    if !message[:time] || !message[:station_id]
      log.warn "Spurious data received"
      return
    end

    sta_id = message[:station_id]

    sta = @stations[sta_id]
    if !sta
      sta = Station.new(name: sta_id, data: message, log: log,
        event_cb: lambda { |sta, event, text, args|
          event(event, "Station #{sta} #{text}", sta_id: sta_id, sta: sta.processed_representation, **args)
        }
      )

      @stations[sta_id] = sta

      event(:STATION_NEW, "New station #{sta.name} found", sta_id: sta_id)
    end

    sta.update(message)

    if !@clock_source
      @clock_internal.stop!
      @clock_source = message[:station_id]
      event(:CLOCK_SYNC, "Clock synced to #{@clock_source}", clock_source: @clock_source)
    end

    if @clock_source == message[:station_id]
      @time = Time.parse(message[:time])
      clock_event
    end
  end

  def rcv_traffic_update(message, delivery_tag:)
    if !message[:station_id]
      log.warn "Spurious data received: missing station_id from TRAFFIC_UPDATE"
      @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: delivery_tag)
      return
    end

    message[:objects].each do |obj_id, data|
      rcv_traffic_update_object(obj_id, data, message[:station_id], delivery_tag)
    end

    @pending_acks[delivery_tag] = true
  end

  def rcv_traffic_update_object(obj_id, data, station_id, delivery_tag)
    obj = @objects[obj_id]
    if !obj
      obj = Obj.new(obj_id: obj_id, data: data, source: station_id, log: log,
        event_cb: lambda { |obj, event, text, args|
          event(event, "Plane #{obj} #{text}", obj_id: obj_id, obj: obj.processed_representation, **args)
        }
      )

      if obj.type == 2
        @towplanes[obj_id] = obj
      end

      if obj.flarm_code
        res = @pg.exec_params("SELECT * FROM planes WHERE flarm_code=$1", [ obj.flarm_code ])
      elsif obj.icao_code
        res = @pg.exec_params("SELECT * FROM planes WHERE icao_code=$1", [ obj.icao_code ])
      end

      if res.ntuples > 0
        obj.plane_id = res[0]['id']
        obj.registration = res[0]['registration']
      else
        res = @pg.exec_params("INSERT INTO planes (uuid,flarm_code,icao_code) VALUES ($1,$2,$3) RETURNING id",
          [ SecureRandom.uuid, obj.flarm_code, obj.icao_code ])

        obj.plane_id = res[0]['id']
      end

      @objects[obj_id] = obj

      event(:OBJECT_NEW, "New object #{obj}, type #{obj.type}", obj_id: obj_id)
    else
      obj.update(data: data, source: station_id)
    end

    @updated_objects[obj.object_id] = obj
  rescue Obj::DataError
  end

  def event(type, text, **data)
    log.info("#{@time}: #{text}")

    if @time && Time.now - @time < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        destination: mycfg.processed_traffic_exchange,
        payload: data.merge({ text: text }),
        routing_key: type.to_s,
        options: {
          type: type.to_s,
          persistent: false,
          mandatory: false,
        }
      )
    end
  end

  def clock_event
    @clock_timeout.reset!

    updates = []
    stations_dump = {}

    @updated_objects.each do |obj_id,obj|
      obj.updates_complete

      @towplanes.each do |towplane_id, towplane|
        towplane.check_towed(obj) if towplane != obj
      end

      if obj.valid?
        updates << {
          at: obj.last_update,
          plane_id: obj.plane_id,
          lat: obj.lat, lng: obj.lng, alt: obj.alt,
          cog: obj.cog, sog: obj.sog, tr: obj.tr, cr: obj.cr,
        }
      end
    end

    if Time.now - @time < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        destination: mycfg.processed_traffic_exchange,
        payload: {
          objects: Hash[@updated_objects.map { |obj_id, obj| [ obj_id, obj.processed_representation ] }],
          stations: Hash[@stations.map { |sta_id, sta| [ sta_id, sta.processed_representation ] }],
        },
        routing_key: 'OBJECTS_UPDATE',
        options: {
          type: 'OBJECTS_UPDATE',
          persistent: false,
          mandatory: false,
        }
      )
    end

    recmsg = Recorder::MsgRecord.new(objects: updates)

    @recorder.tell(recmsg)

    @pending_recs[recmsg.object_id] = @pending_acks.keys

    @pending_acks.clear
    @updated_objects.clear

    periodic_cleanup
  end

end
end
