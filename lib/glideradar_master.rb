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

    @objects = {}
    @stations = {}
  end

  def handle(message)
    case message
    when AM::AMQP::MsgDelivery

      if message.delivery_info.consumer_tag == @msg_consumer
        case message.properties[:type]
        when 'STATION_UPDATE'; rcv_station_update(message)
        when 'TRAFFIC_UPDATE'; rcv_traffic_update(message)

        @amqp.tell AM::AMQP::MsgAck.new(delivery_tag: message.delivery_info.delivery_tag)
      else
        super
      end
    else
      super
    end
  end

  def rcv_station_update(message)
  end

  class Obj
    attr_accessor :type
    attr_accessor :flarm_code
    attr_accessor :last_update

    attr_accessor :srcs

    def initialize(type:)
      @srcs = {}

      @type = type
    end

    def update(data, source)
      now = Time.now

      @last_update = now
      data[:last_update] = now

      @srcs[source] = data
    end
  end

  def rcv_traffic_update(message)
    message[:objects].each do |obj_id, data|
      obj = @objects[obj_id]
      if !obj
        obj = Obj.new(type: data[:type])

        if obj_id =~ /^flarm:(.*)/
          obj.flarm_code = $1
        end

        @objects[obj_id] = obj
      end

      obj.update(data, message[:station_id])
    end

    @timer.reset
  end

  def sync_event
  end

end
end
