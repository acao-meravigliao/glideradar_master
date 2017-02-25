#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

module GlideradarMaster

class Recorder
  include AM::Actor

  class MsgRecord < AM::Msg
    attr_accessor :time
    attr_accessor :traffics
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
      'INSERT INTO acao_radar_points (at, aircraft_id, lat, lng, alt, cog, sog, tr, cr, recorded_at, src, srcs) ' +
      'VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,now(),$10, $11)')
  end

  def actor_handle(message)
    case message
    when MsgRecord
      @pg.transaction do
        message.traffics.each do |tra|
          @pg.exec_prepared('ins',
             [ tra.timestamp, tra.aircraft_id, tra.lat, tra.lng, tra.alt, tra.cog, tra.sog, tra.tr,
               tra.cr, tra.src, tra.srcs.join(',') ])
        end
      end

      actor_reply message, MsgRecordOk.new
    else
      super
    end
  end
end

end
