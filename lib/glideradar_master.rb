#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

require 'ygg/agent/base'

require 'ygg/app/line_buffer'

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

  def agent_boot
    @pg = PG::Connection.open(mycfg.db.to_h)

    @recorder = actor_supervise_new(Recorder, {
      actor_name: :recorder,
      db_config: mycfg.db.to_h,
      monitored_by: self,
    })

    @stats_recorder = actor_supervise_new(StatsRecorder, {
      actor_name: :stats_recorder,
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

    @traffics_by_plane_id = {}
    @traffics_by_flarm_id = {}
    @updated_traffics = {}
    @towplanes = {}
    @stations = {}

    @planes_seen_today = {}
    @today = nil

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
    traffics_to_remove = []

    @traffics_by_plane_id.each do |plane_id, tra|
      begin
        tra.update_time(@time)
      rescue Traffic::DataError
      end

      if @time - tra.last_update > 120.seconds
        tra.remove!
        traffics_to_remove << plane_id
      end
    end

    traffics_to_remove.each do |plane_id|
      @traffics_by_flarm_id.delete @traffics_by_plane_id[plane_id].flarm_id
      @traffics_by_plane_id.delete plane_id
      @towplanes.delete plane_id
    end

    @stations.each do |sta_id, sta|
      begin
        sta.update_time(@time)
      rescue Station::DataError
      end
    end

    if !@today || @today != @time.to_date
      @today = @time.to_date
      @planes_seen_today = {}
    end
  end

  def rcv_station_update(message)
    if !message[:time] || !message[:station_id]
      log.warn "Spurious data received"
      return
    end

    msg_time = Time.parse(message[:time])

    if !@clock_source
      @clock_internal.stop!
      @clock_source = message[:station_id]
      @time = msg_time
      event(:CLOCK_SYNC, "Clock synced to #{@clock_source}", clock_source: @clock_source)
    end

    sta_id = message[:station_id]

    sta = @stations[sta_id]
    if !sta
      sta = Station.new(now: @time, name: sta_id, data: message, log: log,
        event_cb: lambda { |sta, event, text, now, args|
          event(event, "Station #{sta} #{text}", sta_id: sta_id, sta: sta.processed_representation, **args)
        }
      )

      @stations[sta_id] = sta
    end

    sta.update(data: message)

    if @clock_source == message[:station_id]
      @time = msg_time
      clock_event
    end
  end

  def rcv_traffic_update(message, delivery_tag:)
    if !message[:station_id]
      log.warn "Spurious data received: missing station_id from TRAFFIC_UPDATE"
      @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: delivery_tag)
      return
    end

    if !@time
      log.info "Not synced yet, ignoring traffic update"
      @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: delivery_tag)
      return
    end

    message[:objects].each do |flarm_id, data|
      rcv_traffic_update_traffic(flarm_id, data, message[:station_id], delivery_tag)
    end

    @pending_acks[delivery_tag] = true
  end

  def rcv_traffic_update_traffic(flarm_id, data, station_id, delivery_tag)
    plane_id = nil

    tra = @traffics_by_flarm_id[flarm_id]
    if !tra
      res = @pg.exec_params("SELECT * FROM planes WHERE flarm_id=$1", [ flarm_id ])

      plane_data = {}

      if res.ntuples > 0
        plane_id = res[0]['id'].to_i

        plane_data = {
          owner_name: res[0]['owner_name'],
          home_airport: res[0]['home_airport'],
          type_id: nil,#res[0][''],
          type_name: res[0]['type_name'],
          race_registration: res[0]['race_registration'],
          registration: res[0]['registration'],
          common_radio_frequency: res[0]['common_radio_frequency'],
        }
      else
        res = @pg.exec_params("INSERT INTO planes (uuid,flarm_id) VALUES ($1,$2) RETURNING id",
          [ SecureRandom.uuid, flarm_id ])

        plane_id = res[0]['id'].to_i
      end

      tra = Traffic.new(
        now: @time,
        plane_id: plane_id,
        flarm_id: flarm_id,
        plane_data: plane_data,
        data: data,
        source: station_id,
        log: log,
        event_cb: lambda { |tra, event, text, now, args|
          event(event, "Traffic #{tra} #{text}", traffic: tra.traffic_update, **args)
        }
      )

      if tra.type == 2
        @towplanes[plane_id] = tra
      end

      @traffics_by_plane_id[plane_id] = tra
      @traffics_by_flarm_id[flarm_id] = tra
    else
      tra.update(data: data, source: station_id)
    end

    @stats_recorder.tell(StatsRecorder::MsgRecord.new(flarm_id: flarm_id, plane_id: tra.plane_id, data: data,
       station_id: station_id, time: @time))

    @updated_traffics[tra.plane_id] = tra

    if !@planes_seen_today[tra.plane_id]
      res = @pg.exec_params("SELECT * FROM trk_day_planes WHERE day=$1 AND plane_id=$2", [ @time, tra.plane_id ])
      if res.ntuples == 0
        @pg.exec_params("INSERT INTO trk_day_planes (day, plane_id) VALUES ($1,$2) RETURNING id",
          [ @time, tra.plane_id ])
      end

      @planes_seen_today[plane_id] = true
    end

  rescue Traffic::DataError
  end

  def event(type, text, plane_id: nil, **data)
    log.info("#{@time}: #{text}")

    @pg.exec_params('INSERT INTO trk_events (at, event, plane_id, data, text, recorded_at) VALUES ($1,$2,$3,$4,$5,now())',
                    [ @time, type, plane_id, text, data ])

    if @time && Time.now - @time < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        destination: mycfg.processed_traffic_exchange,
        payload: data.merge({ plane_id: plane_id, timestamp: @time, text: text }),
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

    @updated_traffics.each do |plane_id,tra|
      tra.updates_complete

      @towplanes.each do |towplane_id, towplane|
        towplane.check_towed(tra) if towplane != tra
      end

      if tra.valid?
        updates << tra.clone.freeze
      end
    end

    if Time.now - @time < 30.seconds
      @amqp.tell AM::AMQP::MsgPublish.new(
        destination: mycfg.processed_traffic_exchange,
        payload: {
          traffics: Hash[@updated_traffics.map { |plane_id, tra| [ tra.flarm_id, tra.traffic_update ] }],
          stations: Hash[@stations.map { |sta_id, sta| [ sta_id, sta.processed_representation ] }],
        },
        routing_key: 'TRAFFICS_UPDATE',
        options: {
          type: 'TRAFFICS_UPDATE',
          persistent: false,
          mandatory: false,
        }
      )
    end

    recmsg = Recorder::MsgRecord.new(time: @time, traffics: updates)

    @recorder.tell(recmsg)
    @pending_recs[recmsg.object_id] = @pending_acks.keys

    @pending_acks.clear
    @updated_traffics.clear

    periodic_cleanup
  end

end
end
