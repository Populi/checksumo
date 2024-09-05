#!/usr/bin/env ruby

require "time"
require "date"
require "optparse"
require "logging"
require "yaml"
require "fileutils"

require_relative "log_helper"
require_relative "mysql_connection"
require_relative "table"
require_relative "watcher"

# Constants
FLAGS = Mysql2::Client::REMEMBER_OPTIONS |
  Mysql2::Client::LONG_PASSWORD |
  Mysql2::Client::LONG_FLAG |
  Mysql2::Client::PROTOCOL_41 |
  Mysql2::Client::SECURE_CONNECTION |
  Mysql2::Client::MULTI_STATEMENTS

DEFAULT_GROUP_CONCAT_MAX_LENGTH = 1_000_000

# Command-line Parser
class Parser
  def self.parse(options, opts = {})
    defaults = opts.fetch(:defaults)
    args = {
      chunk_size: DEFAULT_CHUNK_SIZE,
      group_concat_max_length: DEFAULT_GROUP_CONCAT_MAX_LENGTH,
      master_hostname: "127.0.0.1",
      master_port: 3060,
      master_user_name: ENV["DB_USER"],
      master_password: ENV["DB_PASS"],
      replica_hostname: "127.0.0.1",
      replica_port: 3070,
      replica_user_name: ENV["DB_USER"],
      replica_password: ENV["DB_PASS"],
      verbose: :info,
      timeout: 10,
      watch_mode: :ROW_DIFF,
      wait_interval: 5,
      log_dir: "./logs"
    }

    args.merge!(defaults) if defaults

    # Needs to be calculated, rather than persisted
    args[:deadline] = Time.now + args[:timeout] * 60

    opt_parser = OptionParser.new do |opts|
      opts.banner = "Usage: checksumo.rb [options] [table names]"

      opts.on("-h", "--help", "Print help message") do
        puts opts
        exit
      end
      opts.on("-v", "--[no-]verbose", "Run verbosely") do |v|
        args[:verbose] = if v
                           :debug
                         else
                           :warn
                         end
      end
      opts.on("--database=DB_NAME", "Database name REQUIRED") do |n|
        args[:database_name] = n
      end
      opts.on("--master-host=HOSTNAME", "Master DB hostname [#{args[:master_hostname]}]") do |n|
        args[:master_hostname] = n
      end
      opts.on("--master-port=PORT", "Master DB port [3060]") do |n|
        args[:master_port] = n
      end
      opts.on("--master-username=USERNAME", 'Master DB user name [$ENV["DB_USER"]]') do |n|
        args[:master_user_name] = n
      end
      opts.on("--master-password=USERNAME", 'Master DB user password [$ENV["DB_PASS"]]') do |n|
        args[:master_password] = n
      end
      opts.on("--replica-host=HOSTNAME", "Replica DB hostname [#{args[:replica_hostname]}]") do |n|
        args[:replica_hostname] = n
      end
      opts.on("--replica-port=HOSTNAME", "Replica DB port [#{args[:replica_port]}]") do |n|
        args[:replica_port] = n
      end
      opts.on("--replica-username=USERNAME", 'Replica DB user name [$ENV["DB_USER"]]') do |n|
        args[:replica_user_name] = n
      end
      opts.on("--replica-password=USERNAME", 'Replica DB user password [$ENV["DB_PASS"]]') do |n|
        args[:replica_password] = n
      end
      opts.on("--timeout=DURATION", "Timeout in minutes [#{args[:timeout]}]") do |min|
        args[:deadline] = Time.now + min.to_f * 60
      end
      opts.on("--wait-interval=DURATION", "Wait interval between table checks [#{args[:wait_interval]}]") do |int|
        args[:wait_interval] = int.to_f
      end
      opts.on("--watch-mode=MODE", "Watch mode: one of (CHUNK_SUMMARY, ROW_DIFF, WAIT) [#{args[:watch_mode]}]") do |m|
        if /^ROW/i =~ m
          args[:watch_mode] = :ROW_DIFF
        elsif /^WAIT/i =~ m
          args[:watch_mode] = :WAIT
        elsif /^CHUNK/i =~ m
          args[:watch_mode] = :CHUNK_SUMMARY
        else
          puts "watch mode must be one of [CHUNK_SUMMARY, ROW_DIFF, WAIT]"
          exit
        end
      end
      opts.on("--logdir=DIRECTORY", "Directory for output logs [./logs]") do |dir|
        args[:log_dir] = dir
      end
      opts.on("--chunk-size=SIZE", "Table chunk size for checksum comparisons [#{args[:chunk_size]}]") do |size|
        args[:chunk_size] = size.to_i
      end
      opts.on("--group_concat_max_length=SIZE", "Group concat buffer size [#{args[:group_concat_max_length]}]") do |size|
        args[:group_concat_max_length] = size.to_i
      end
    end
    opt_parser.parse!(options)
    args
  end
end

include LogHelper

logger = nil

def setup(opts = {})
  gcml = opts.fetch(:group_concat_max_length, DEFAULT_GROUP_CONCAT_MAX_LENGTH)
  # reduce row- and table-locking to the least possible ACID compliance
  # see https://www.rubydoc.info/gems/mysql2/#initial-command-on-connect-and-reconnect
  init_command = %(SET @@SESSION.transaction_isolation = 'READ-UNCOMMITTED', @SESSION.transaction_read_only = '1', group_concat_max_len = #{gcml})

  master_client = Mysql2::Client.new(
    host: opts[:master_hostname],
    port: opts[:master_port],
    flags: FLAGS,
    username: opts[:master_user_name],
    password: opts[:master_password],
    database: opts[:database_name],
    init_command: init_command
  )

  replica_client = Mysql2::Client.new(
    host: opts[:replica_hostname],
    port: opts[:replica_port],
    flags: FLAGS,
    username: opts[:replica_user_name],
    password: opts[:replica_password],
    database: opts[:database_name],
    init_command: init_command
  )

  @logger.debug("table names: #{opts.fetch(:table_names, [])}")

  ReplicationWatcher.new(
    master: MysqlConnection.new(client: master_client, database_name: opts[:database_name]),
    replica: MysqlConnection.new(client: replica_client, database_name: opts[:database_name]),
    database_name: opts[:database_name],
    table_pairs: [],
    table_names: opts.fetch(:table_names, []),
    chunk_size: opts[:chunk_size]
  )
end

def set_alarm(opts = {})
  # Perl pretending to be Ruby...
  return unless opts[:deadline]

  deadline = opts[:deadline]
  logger.info("setting timeout alarm for '#{deadline}'")

  Signal.trap("ALRM") do
    logger.error("caught SIGALRM, timing out")

    # message on STDOUT/STDERR too
    puts "Caught ALARM set for '#{deadline}', timing out!"

    exit!(1)
  end

  Thread.new do
    # This isn't perfect, but it's close enough for our purposes.
    while Time.now < deadline
      sleep 1
    end
    Process.kill("ALRM", $$)
  end
end

# This probably belongs in a BEGIN block
def set_sig_trap(_opts = {})
  # This is pretty ugly.
  # Each signal has to be trapped individually, so we'll just reuse a common Proc to handle them all
  trap_handler = proc do |signo|
    signame = Signal.signame(signo)

    logger.error("Caught #{signame}, bailing out")
    puts "Caught #{signame}, exiting!"

    exit!(signo)
  end

  Signal.trap("SIGINT", trap_handler)
  Signal.trap("SIGTERM", trap_handler)
end

def parse_config
  defaults = Hash[]
  return defaults unless File.exist?("checksumo.yml")
  persisted = YAML.parse_file("checksumo.yml").to_ruby
  persisted["defaults"].each do |k, v|
    defaults[k.to_sym] = v
  end
  defaults
rescue
  Hash[]
end

# Main entry point
def main(args)
  # Find default settings in YAML config
  defaults = parse_config

  # Parse the parse-able options
  options = Parser.parse(args, defaults: defaults)

  # Check the rest of ARGV to find table names
  table_names = args
  options[:table_names] = table_names

  # Ensure logger is logging at the correct level
  log_level = options.fetch(:verbose, :info)
  LogHelper::LogConfig.init(level: log_level, directory: options[:log_dir])
  logger = logger(name: "checksumo")
  logger.debug("logger: #{logger.inspect}")

  # Set a timeout alarm before we do anything else
  set_alarm(options)

  # set up Signal Trap
  set_sig_trap()

  # Create a watcher
  watcher = setup(options)
  watcher.search if watcher.table_pairs.empty?

  # Watch mode should be a symbol
  watch_mode = options[:watch_mode]
  if watch_mode.is_a? String
    watch_mode = watch_mode.to_sym
    @logger.debug("coerced watch_mode to symbol")
  end

  if watch_mode == :WAIT
    # wait to see if the tables become aligned
    watcher.watch(options)
  elsif watch_mode == :ROW_DIFF
    # create a row-level reconcile report and exit
    watcher.reconcile
  else
    # Default path: create a chunk_checksum report and exit
    watcher.reconcile_chunks
  end
end

if __FILE__ == $PROGRAM_NAME
  main(ARGV)
end
