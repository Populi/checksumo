require "memoist"
require "logging"
require_relative "log_helper"
require_relative "mysql_connection"

DEFAULT_CHUNK_SIZE = 1024

# Encapsulate comparison logic
class AbstractComparison
  attr_accessor :master, :replica

  def initialize(opts = {})
    @master = opts[:master]
    @replica = opts[:replica]
  end

  def master?
    !@master.nil?
  end

  def replica?
    !@replica.nil?
  end

  def primary_key
    @master&.primary_key || @replica&.primary_key
  end

  def compare
    master.equal?(replica)
  end
end

# Encapsulate chunk comparison logic
class ChunkComparison < AbstractComparison
  attr_accessor :min_row, :max_row, :table_name

  def initialize(opts = {})
    super
    @max_row = opts[:max_row]
    @min_row = opts[:min_row]
    @table_name = opts[:table_name]
  end
end

# Encapsulate row comparison logic
class RowComparison < AbstractComparison
  attr_accessor :row_id, :table_name

  def initialize(opts = {})
    super
    @row_id = opts[:row_id]
    @table_name = opts[:table_name]
  end

  def self.from(opts = {})
    master = opts[:master]
    replica = opts[:replica]
    raise "must provide :master or :replica" unless master || replica

    opts[:row_id] = master&.row_id || replica.row_id
    opts[:table_name] = master&.table_name || replica.table_name
    RowComparison.new(opts)
  end
end

# Encapsulate per-table logic
class Table
  extend Memoist
  include LogHelper

  attr_accessor :conn

  def initialize(table_name, connection, opts = {})
    @conn = connection
    @name = table_name
    @logger = opts.fetch(:logger) do
      logger
    end
  end

  def max_row_id
    @conn.max_row_id(@name)
  end

  memoize :max_row_id

  def min_row_id
    @conn.min_row_id(@name)
  end

  memoize :min_row_id

  def chunk_checksum(opts = {})
    @conn.chunk_checksum(@name, opts)
  end

  def row_checksum(opts = {})
    @conn.row_checksum(@name, opts)
  end

  def primary_key
    @conn.primary_key(@name)
  end

  memoize :primary_key

  def row_values(row_id)
    @conn.row_values(@name, row_id)
  end
end

# Encapsulate table-pair (master:slave) logic
class TablePair
  extend Memoist
  include LogHelper

  attr_accessor :database_name, :master, :replica, :table_name

  def initialize(table_name, master_connection, replica_connection, opts = {})
    @table_name = table_name
    @master = Table.new(table_name, master_connection, opts)
    @replica = Table.new(table_name, replica_connection, opts)
    @chunk_size = opts.fetch(:chunk_size, DEFAULT_CHUNK_SIZE)
    @database_name = opts.fetch(:database_name, nil)
    @logger = opts.fetch(:logger) do
      logger
    end
  end

  def delta(opts = {})
    ccs = compare_chunks(opts)
    if ccs.empty?
      @logger.debug("no chunk diff on #{@table_name}, skipping row diff")
      return Hash[]
    end

    row_diff = Hash[]
    ccs.each do |cc|
      rd = compare_rows(min: cc.master.min, max: cc.master.max)
      row_diff.merge! rd
    end

    @logger.debug("returning diff: #{row_diff}")

    row_diff
  end

  def compare_rows(opts = {})
    diff = Hash[]
    @master.row_checksum(opts).each do |cs|
      diff[cs.row_id] = RowComparison.from(master: cs)
    end
    @replica.row_checksum(opts).each do |cs|
      mcs = diff[cs.row_id]

      if mcs.nil?
        diff[cs.row_id] = RowComparison.from(replica: cs)
        next
      end

      if mcs.master.equal?(cs)
        diff.delete(cs.row_id)
        next
      end

      diff[cs.row_id].replica = cs
    end
    diff
  end

  def compare_chunks(opts = {})
    diff = []
    primary_key = @master.primary_key
    master_chunks = master_chunks(opts)
    master_chunks.each do |mch|
      # This should only be one chunk, but it's technically a list, so we'll treat it like a list
      @replica.chunk_checksum(min: mch.min, max: mch.max).each do |rch|
        next if rch.equal?(mch) # only keep checksums that are mismatched

        diff << ChunkComparison.new(master: mch,
          replica: rch,
          table_name: mch.table_name,
          primary_key: primary_key,
          min_row: mch.min,
          max_row: mch.max)
      end
    end

    if @logger.debug?
      summary = {
        table_name: @table_name,
        max_row_id: @master.max_row_id,
        min_row_id: @master.min_row_id,
        primary_key: @master.primary_key,
        diff: diff
      }

      @logger.debug("checksum diff: #{summary}")
    end

    diff
  end

  def generate_update(row_id)
    delta = row_diff(row_id)

    return "" if delta.empty?

    table_name = if @database_name
      "#{@database_name}.#{@table_name}"
    else
      @table_name
    end

    primary_key = @master.primary_key
    pairs = delta.filter { |k, v| !k.eql?(primary_key) }.map do |k, v|
      val = if v.nil?
        "NULL"
      else
        %('#{v}')
      end
      %(#{k} = #{val})
    end
    %(UPDATE #{table_name} SET #{pairs.join(",\n\t\t")}
         WHERE #{primary_key} = '#{row_id}'\n;)
  end

  private

  def master_chunks(_opts = {})
    chunks = []
    row_id = @master.min_row_id
    loop do
      @master.chunk_checksum(min: row_id, limit: @chunk_size).each do |mch|
        chunks.unshift(mch)
      end
      @logger.debug("chunks: #{chunks}")
      row_id = chunks.first.max
      break if chunks.first.count < @chunk_size
    end

    chunks.reverse
  end

  # Find a per-column diff for a table pair
  def row_diff(row_id)
    master_row = @master.row_values(row_id).first
    replica_row = @replica.row_values(row_id).first
    delta = Hash[]
    master_row.each do |k, v|
      next if replica_row[k] == v
      delta[k] = v
    end
    delta
  end
end
