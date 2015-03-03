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

require 'serialport'

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
    @amqp.ask(AM::AMQP::MsgDeclareExchange.new(
      name: mycfg.exchange,
      type: :topic,
      options: {
        durable: true,
        auto_delete: false,
      }
    )).value
  end
end
