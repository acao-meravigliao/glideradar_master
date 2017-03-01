#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'securerandom'
require 'time'

require 'pg'

require 'glideradar_master/version'
require 'glideradar_master/task'
require 'glideradar_master/station'
require 'glideradar_master/traffic'
require 'glideradar_master/recorder'
require 'glideradar_master/stats_recorder'

module GlideradarMaster

class App < Ygg::Agent::Base
  self.app_name = 'glideradar_master'
  self.app_version = VERSION
  self.task_class = Task

  def prepare_default_config
    app_config_files << File.join(File.dirname(__FILE__), '..', 'config', 'glideradar_master.conf')
    app_config_files << '/etc/yggdra/glideradar_master.conf'
  end

  def prepare_options(o)
    super

    o.on('--msg-dump-dir DIR', 'Create a daily dump of raw received messages') { |v| config['glideradar_master.msg_dump_dir'] = v }
    o.on('--enable-old-broadcast', 'Enable broadcasting on processed traffic exchange of old updates') { |v| config['glideradar_master.enable_old_broadcast'] = true }
  end

  def agent_boot
    @pg = PG::Connection.open(mycfg.db.to_h)
    @pg.type_map_for_results = PG::BasicTypeMapForResults.new(@pg)

    @recorder = actor_supervise_new(Recorder, config: {
      actor_name: :recorder,
      db_config: mycfg.db.to_h,
    })

    @stats_recorder = actor_supervise_new(StatsRecorder, config: {
      actor_name: :stats_recorder,
      db_config: mycfg.db.to_h,
    })

    @amqp.ask(AM::AMQP::MsgExchangeDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.source_exchange,
      type: :topic,
      durable: true,
      auto_delete: false,
    )).value

    @amqp.ask(AM::AMQP::MsgQueueDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.queue,
      durable: true,
      auto_delete: false,
      arguments: {
        :'x-message-ttl' => (3 * 86400 * 1000),
      },
    )).value

    @amqp.ask(AM::AMQP::MsgQueueBind.new(
      channel_id: @amqp_chan,
      queue_name: mycfg.queue,
      exchange_name: mycfg.source_exchange,
      routing_key: '#'
    )).value

    @msg_consumer = @amqp.ask(AM::AMQP::MsgConsume.new(
      channel_id: @amqp_chan,
      queue_name: mycfg.queue,
      send_to: self.actor_ref,
    )).value.consumer_tag

    @processed_exchange = @amqp.ask(AM::AMQP::MsgExchangeDeclare.new(
      channel_id: @amqp_chan,
      name: mycfg.processed_traffic_exchange,
      type: :topic,
      durable: true,
      auto_delete: false,
    )).value

    @airfields = {}

    res = @pg.exec_params("SELECT acao_airfields.*, core_locations.lat, core_locations.lng, core_locations.alt "+
                           "FROM acao_airfields JOIN core_locations ON acao_airfields.location_id=core_locations.id")
    res.each do |airfield|

log.warn "AIRFIELD = #{airfield}"

      airfield.symbolize_keys!
      @airfields[airfield[:id]] = airfield
    end

    @pending_recs = {}
    @pending_acks = {}

    @traffics_by_aircraft_id = {}
    @traffics_by_flarm_id = {}
    @updated_traffics = {}
    @towplanes = {}
    @stations = {}

    @today = nil

    @clock_source = nil
    @clock_timeout = delay(5.seconds) do
      event(:LOST_CLOCK, "Lost clock #{@clock_source}", clock_source: @clock_source)
      @clock_source = nil
    end

    open_dump_file if config.glideradar_master.msg_dump_dir
  end

  def actor_handle(message)
    case message
    when AM::AMQP::MsgDelivery

      if message.consumer_tag == @msg_consumer
        if @dump_file
          @dump_file.write({ headers: message.headers, payload: message.payload }.to_json + "\n")
        end

        case message.headers[:type]
        when 'STATION_UPDATE'
          payload = JSON.parse(message.payload).deep_symbolize_keys!
          rcv_station_update(payload)
          @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: message.delivery_tag)
        when 'TRAFFIC_UPDATE'
          payload = JSON.parse(message.payload).deep_symbolize_keys!
          rcv_traffic_update(payload, delivery_tag: message.delivery_tag)
        else
          @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: message.delivery_tag)
        end
      else
        super
      end
    when Recorder::MsgRecordOk, Recorder::MsgRecordFailure
      delivery_tags = @pending_recs[message.in_reply_to.object_id]
      if delivery_tags
        delivery_tags.each do |delivery_tag|
          @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: delivery_tag)
        end
      else
        log.err "Unable to find message to acknowledge!"
      end
    else
      super
    end
  end

  def rcv_station_update(message)
    if !message[:time] || !message[:station_id]
      log.warn "Spurious data received"
      return
    end

    msg_time = Time.parse(message[:time])

    if !@clock_source
      @clock_source = message[:station_id]
      @now = msg_time
      event(:CLOCK_SYNC, "Clock synced to #{@clock_source}", clock_source: @clock_source)
    end

    sta_id = message[:station_id]

    sta = @stations[sta_id]
    if !sta
      sta = Station.new(now: @now, name: sta_id, data: message, log: log,
        event_cb: lambda { |sta, event, text, now, args|
          event(event, "Station #{sta} #{text}", sta_id: sta_id, sta: sta.processed_representation, **args)
        }
      )

      @stations[sta_id] = sta
    end

    sta.update(data: message)

    if @clock_source == message[:station_id]
      @now = msg_time
      clock_event
    end
  end

  def rcv_traffic_update(message, delivery_tag:)
    if !message[:station_id]
      log.warn "Spurious data received: missing station_id from TRAFFIC_UPDATE"
      @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: delivery_tag)
      return
    end

    if !@now
      log.info "Not synced yet, ignoring traffic update"
      @amqp.tell AM::AMQP::MsgAck.new(channel_id: @amqp_chan, delivery_tag: delivery_tag)
      return
    end

    message[:objects].each do |flarm_id, data|
      rcv_traffic_update_traffic(flarm_id, data, message[:station_id], delivery_tag)
    end

    @pending_acks[delivery_tag] = true
  end

  def rcv_traffic_update_traffic(flarm_id, data, station_id, delivery_tag)

    data = data.dup
    data[:src] = station_id
    data[:ts] = Time.parse(data[:ts])
    data[:rcv_ts] = Time.new

    tra = @traffics_by_flarm_id[flarm_id]
    if !tra
      if !(match = /^(flarm|icao):(.*)$/.match(flarm_id))
        return
      end

      flarm_identifier_type = match[1]
      flarm_identifier = match[2]

      tra = Traffic.new(
        now: @now,
        flarm_identifier_type: flarm_identifier_type,
        flarm_identifier: flarm_identifier,
        airfields: @airfields.deep_dup,
        data: data,
        pg: @pg,
        log: log,
        event_cb: lambda { |tra, event, text, now, args|
          event(event, "Traffic #{tra} #{text}", traffic: tra.traffic_update, **args)
        }
      )

      if tra.type == 2
        @towplanes[tra.aircraft_id] = tra
      end

      @traffics_by_aircraft_id[tra.aircraft_id] = tra
      @traffics_by_flarm_id[flarm_id] = tra
    else
      tra.update(data: data)
    end

    @stats_recorder.tell(StatsRecorder::MsgRecord.new(flarm_id: flarm_id, aircraft_id: tra.aircraft_id, data: data,
       station_id: station_id, time: @now))

    @updated_traffics[tra.aircraft_id] = tra

  rescue Traffic::DataError
  end

  def event(type, text, aircraft_id: nil, **data)
    log.info("#{@now}: #{text}")

    return if !@now

    @pg.exec_params('INSERT INTO acao_radar_events (at, event, aircraft_id, data, text, recorded_at) VALUES ($1,$2,$3,$4,$5,now())',
                    [ @now, type, aircraft_id, text, data ])

    if @now && Time.now - @now < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        channel_id: @amqp_chan,
        exchange: mycfg.processed_traffic_exchange,
        payload: data.merge({ aircraft_id: aircraft_id, timestamp: @now, text: text }).to_json,
        routing_key: type.to_s,
        persistent: false,
        mandatory: false,
        headers: {
          type: type.to_s,
          content_type: 'application/json',
        }
      )
    end
  end

  def clock_event
    @clock_timeout.reset!

    updates = []
    stations_dump = {}

    @traffics_by_aircraft_id.each do |aircraft_id,tra|
      tra.clock_event(@now)
    end

    @updated_traffics.each do |aircraft_id,tra|
      @towplanes.each do |towplane_id, towplane|
        towplane.check_towed(tra) if towplane != tra
      end

      if tra.valid?
        updates << tra.clone.freeze
      end
    end

    @stations.each do |sta_id, sta|
      begin
        sta.clock_event(@now)
      rescue Station::DataError
      end
    end

    # Publish the traffic update if not too old
    if config.glideradar_master.enable_old_broadcast || Time.now - @now < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        channel_id: @amqp_chan,
        exchange: mycfg.processed_traffic_exchange,
        payload: {
          traffics: Hash[@updated_traffics.map { |aircraft_id, tra| [ tra.flarm_combined_identifier, tra.traffic_update ] }],
          stations: Hash[@stations.map { |sta_id, sta| [ sta_id, sta.processed_representation ] }],
        }.to_json,
        routing_key: 'TRAFFICS_UPDATE',
        persistent: false,
        mandatory: false,
        headers: {
          type: 'TRAFFICS_UPDATE',
          content_type: 'application/json',
        }
      )
    end

    # Record the track
    recmsg = Recorder::MsgRecord.new(time: @now, traffics: updates)

    @recorder.tell(recmsg)
    @pending_recs[recmsg.object_id] = @pending_acks.keys

    @pending_acks.clear
    @updated_traffics.clear

    @today ||= @now.to_date
    if @today != @now.to_date
      @today = @now.to_date
      day_changed!
    end

    periodic_cleanup
  end

  def periodic_cleanup
    traffics_to_remove = []

    @traffics_by_aircraft_id.each do |aircraft_id, tra|
      if @now - tra.timestamp > 120.seconds
        tra.remove!
        traffics_to_remove << aircraft_id
      end
    end

    traffics_to_remove.each do |aircraft_id|
      @traffics_by_flarm_id.delete @traffics_by_aircraft_id[aircraft_id].flarm_combined_identifier
      @traffics_by_aircraft_id.delete aircraft_id
      @towplanes.delete aircraft_id
    end
  end

  def day_changed!
    log.warn "------------- Day Changed --------------"

    # Cleanup timetable, removing flights with no takeoff/landing

    open_dump_file if config.glideradar_master.msg_dump_dir
  end

  def open_dump_file
    @dump_file.close if @dump_file
    @dump_file = File.open(File.join(config.glideradar_master.msg_dump_dir, Time.now.strftime('dump-%Y%m%d')), 'ab')
  end

end
end
