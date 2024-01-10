require "logging"

class LogHelper
  def self.init(args = {})
    log_dir = args.fetch(:log_dir, File.absolute_path("."))
    @@directory = File.absolute_path(log_dir)
    @@level = args.fetch(:level, :info)

    FileUtils.mkdir_p @@directory unless File.exist? File.absolute_path(@@directory)

    @@logger = Logging.logger["root_logger"]
    @@logger.level = args.fetch(:level, :info)

    file_appender = Logging::Appenders::RollingFile.new("root_log_file", filename: "#{log_dir}/checksumo{{.%d}}.log", age: "daily")
    @@logger.add_appenders(file_appender)

    @@logger
  end

  def self.logger(args = {})
    name = args.fetch(:name, "#{self.class}")
    level = args.fetch(:level, @@level)
    path = "#{@@directory}/#{name}{{.%d}}.log"

    logger = Logging.logger["#{name}_logger"]
    logger.level = level
    file_appender = Logging::Appenders::RollingFile.new("#{name}_log_file", filename: path, age: "daily")
    logger.add_appenders(file_appender)
  end
end
