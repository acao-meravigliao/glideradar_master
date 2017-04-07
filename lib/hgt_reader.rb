#
# Copyright (C) 2017-2017, Daniele Orlandi
#
# Author:: Daniele Orlandi <daniele@orlandi.com>
#
# License:: You can redistribute it and/or modify it under the terms of the LICENSE file.
#

class HGTReader
  class NoFile < StandardError ; end
  class InvalidFile < StandardError ; end

  class CacheEntry
    attr_accessor :file
    attr_accessor :resolution
  end

  def initialize(dir:)
    @dir = dir

    @cache = {}
  end

  def degrees_to_dms(float_val)
    d, remainder = number_split(float_val.abs)
    m, remainder = number_split(remainder * 60.0)
    s, remainder = number_split(remainder * 60.0)
    s += remainder.round
    return [ float_val < 0 ? -d : d, m, s ]
  end

  def number_split(float_val)
    top = float_val.truncate
    return [ top, float_val - top ]
  end

  def filename(lat, lng)
    "%c%02d%c%03d.hgt" % [
      lat > 0 ? 'N' : 'S',
      lat.truncate,
      lng > 0 ? 'E' : 'W',
      lng.truncate
    ]
  end

  def with_file(lat, lng, &block)
    fn = filename(lat, lng)

    ce = @cache[fn]
    if !ce
      ce = @cache[fn] = CacheEntry.new

      begin
        ce.file = File.open(File.join(@dir, fn), 'rb')
      rescue Errno::ENOENT
        raise NoFile
      end

      case ce.file.size
      when 2884802
        ce.resolution = 3
      when 25934402
        ce.resolution = 1
      else
        raise InvalidFile
      end
    end

    block.call(ce)
  end

  def height(lat, lng)
    lat_dms = degrees_to_dms(lat)
    lng_dms = degrees_to_dms(lng)

    res = nil

    with_file(lat, lng) do |f|
      y = (3600 - (lat_dms[1] * 60 + lat_dms[2])) / f.resolution
      x = (lng_dms[1] * 60 + lng_dms[2]) / f.resolution

      f.file.seek((y * ((3600 / f.resolution) + 1) * 2) + (x * 2))

      res = f.file.read(2).unpack('s>')[0]
    end

    res
  end

  def close_all
    @cache.each do |f|
      f.file.close
    end

    @cache = {}
  end
end
