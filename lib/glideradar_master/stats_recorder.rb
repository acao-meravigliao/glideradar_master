#
# Copyright (C) 2015-2015, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

module GlideradarMaster

class StatsRecorder
  include AM::Actor

  class MsgRecord < AM::Msg
    attr_accessor :flarm_id
    attr_accessor :data
    attr_accessor :plane_id
    attr_accessor :station_id
    attr_accessor :time
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
      'INSERT INTO trk_track_stats (at, rcv_at, rec_at, src, plane_id, lat, lng, alt, cog, sog, tr, cr) ' +
      'VALUES ($1,$2,now(),$3,$4,$5,$6,$7,$8,$9,$10,$11)')
  end

  def actor_handle(message)
    case message
    when MsgRecord
      @pg.exec_prepared('ins',
         [ message.data[:ts], message.data[:rcv_ts], message.station_id, message.plane_id,
           message.data[:lat], message.data[:lng], message.data[:alt], message.data[:cog], message.data[:sog],
           message.data[:tr], message.data[:cr] ])

      reply message, MsgRecordOk.new
    else
      super
    end
  end
end

end
