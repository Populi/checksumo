require "logging"
require "fileutils"
require "memoist"

module LogHelper
  extend Memoist

  def logger(opts = {})
    @logger ||= begin
      name = opts.fetch(:name, self.class.to_s)
      LogConfig.logger(name, opts)
    end
  end

  memoize :logger

  class LogConfig
    @@loggers = Hash[]
    @@directory = ""
    @@level = :info

    def self.loggers
      @@loggers
    end

    def self.directory
      @@directory
    end

    def self.level
      @@level
    end

    def self.root_logger
      @@loggers["root_logger"]
    end

    def self.init(opts = {})
      log_dir = opts.fetch(:directory, File.absolute_path("."))
      @@directory = File.absolute_path(log_dir)
      @@level = opts.fetch(:level, :info)

      FileUtils.mkdir_p @@directory unless File.exist? File.absolute_path(@@directory)

      name = "root_logger"
      root_logger = Logging.logger[name]
      root_logger.level = opts.fetch(:level, :info)

      file_appender = Logging::Appenders::RollingFile.new("root_log_file", filename: "#{directory}/checksumo{{.%d}}.log", age: "daily")
      root_logger.add_appenders(file_appender)

      @@loggers[name] = root_logger
    end

    def self.logger(name, opts = {})
      return @@loggers[name] unless @@loggers[name].nil?

      log_dir = opts.fetch(:directory, @@directory)
      level = opts.fetch(:level, @@level)

      directory = File.absolute_path(log_dir)
      FileUtils.mkdir_p directory unless File.exist? File.absolute_path(directory)

      file_appender = Logging::Appenders::RollingFile.new("root_log_file", filename: "#{directory}/#{name}{{.%d}}.log", age: "daily")

      log = Logging.logger["#{name}_logger"]
      log.level = opts.fetch(:level, :info)
      log.add_appenders(file_appender)

      @@loggers[name] = log

      log
    end
  end
end
