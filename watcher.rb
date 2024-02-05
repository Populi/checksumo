require "logging"
require "memoist"
require_relative "log_helper"
require_relative "mysql_connection"
require_relative "table"

DEFAULT_WAIT_INTERVAL = 5

# Watch pairs of tables for replication errors
class ReplicationWatcher
  extend Memoist
  include LogHelper

  attr_accessor :database_name, :master, :replica, :table_pairs

  def initialize(opts = {})
    @master = opts.fetch(:master)
    @replica = opts.fetch(:replica)
    @database_name = opts.fetch(:database_name, nil)
    @table_pairs = opts.fetch(:table_pairs, [])

    @logger = opts.fetch(:logger) do
      logger
    end

    table_names = opts.fetch(:table_names, [])
    table_names.each do |n|
      next unless n.match?('^[A-Za-z_\.]+$')
      @logger.debug("adding table pair for #{n}")
      @table_pairs << TablePair.new(n, @master, @replica, database_name: @database_name)
    end

    @logger.debug("initialized ReplicationWatcher: #{self}")
  end

  def reset(table_names, opts = {})
    @table_pairs = []
    table_names.each do |t|
      @logger.debug("creating TablePair from '#{t.inspect}'")
      @table_pairs << TablePair.new(t, @master, @replica, database_name: @database_name)
    end
    @table_pairs.uniq! { |tp| tp.table_name }
  end

  def search
    @master.search.each_key do |tablename|
      @table_pairs << TablePair.new(tablename, @master, @replica, database_name: @database_name)
    end
  end

  def delta
    diff = []
    @table_pairs.each do |tp|
      @logger.debug("searching for delta on #{tp.table_name}")

      d = tp.delta
      next if d.empty?

      d.each_value do |v|
        yield v if block_given?

        diff.push v
      end
    end

    diff
  end

  def generate_delete(row_comparison)
    return "" unless !row_comparison.master? && row_comparison.replica?

    @logger.info("generating DELETE commands for #{row_comparison}")

    cmds = replica.generate_delete(row_comparison.table_name, row_comparison.row_id)
    return "" if cmds.empty?

    <<~COMMAND
      -- run on REPLICA
      #{cmds.join("\n\n")}
    COMMAND
  end

  def generate_insert(row_comparison)
    return "" unless row_comparison.master? && !row_comparison.replica?

    cmds = master.generate_insert(row_comparison.table_name, row_comparison.row_id)
    return "" if cmds.empty?

    <<~COMMAND
      -- run on REPLICA
      #{cmds.join("\n\n")}
    COMMAND
  end

  def generate_update(row_comparison)
    return "" unless row_comparison.master? && row_comparison.replica?

    # UPDATE is difficult: we can just blindly create an UPDATE from master, but that can be costly for indexing
    # it's less expensive to update ONLY the columns that need to differ... which means we need to query both instances
    #

    tp = table_pairs.find { |tp| tp.table_name == row_comparison.table_name }
    return "" if tp.nil?

    cmds = tp.generate_update(row_comparison.row_id)

    # cmds = master.generate_update(row_comparison.table_name, row_comparison.row_id)
    return "" if cmds.empty?

    <<~COMMAND
      -- run on REPLICA
      #{cmds}\n\n
    COMMAND
  end

  def reconcile_chunks(opts = {})
    diff = @table_pairs.flat_map do |tp|
      @logger.debug("searching for chunk delta on #{tp.table_name}")
      tp.compare_chunks(opts)
    end

    diff.each do |cc|
      @logger.debug("found chunk_checksum diff in delta: #{cc.inspect}")

      puts "diff found on table #{cc.table_name} where #{cc.table_name}.#{cc.primary_key} between '#{cc.min_row}' and '#{cc.max_row}'"
    end
  end

  def reconcile
    delta do |rc|
      @logger.debug("found row_checksum in delta: #{rc.inspect}")

      cmd = generate_delete(rc)
      puts cmd unless cmd.nil?

      cmd = generate_insert(rc)
      puts cmd unless cmd.nil?

      cmd = generate_update(rc)
      puts cmd unless cmd.nil?
    end
  end

  def watch(opts = {})
    time_to_bail = false

    # If we're in this watching mode, we need to catch ALRM signals here instead of exiting
    # Setting another Signal trap replaces the earlier trap.
    Signal.trap("ALRM") do
      @logger.error("caught ALRM signal, timing out")

      # message on STDOUT/STDERR too
      puts "Caught ALARM, timing out!"

      time_to_bail = true
    end

    row_diff = delta
    puts "watch -ing row_diff: #{row_diff}"

    sleep_time = opts.fetch(:wait_interval, DEFAULT_WAIT_INTERVAL)

    loop do
      break if row_diff&.empty?
      break if time_to_bail

      table_names = row_diff.map { |rc| rc.table_name }.uniq

      puts "found #{row_diff.size} rows differing between MASTER and REPLICA on #{table_names}, sleeping #{sleep_time} seconds to allow replication"
      @logger.debug("sleeping #{sleep_time} seconds to allow replication, row_diff: #{row_diff}")

      reset(table_names, opts)
      @logger.debug("updated tables to check: #{@table_names}")

      sleep sleep_time
      row_diff = delta
    end

    reconcile
  end
end
