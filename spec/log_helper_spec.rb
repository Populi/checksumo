require "rspec"
require_relative "../log_helper"

class TestClass
  include LogHelper
end

describe "LogHelper" do
  describe "class methods" do
    subject { LogHelper::LogConfig }
    it { should respond_to(:directory) }
    it { should respond_to(:logger) }
    it { should respond_to(:level) }
    it { should respond_to(:loggers) }
    it { should respond_to(:root_logger) }
  end
  context "when #init is called" do
    subject { TestClass.new() }
    it "creates a new root_logger" do
      LogHelper::LogConfig.init()
      expect(LogHelper::LogConfig.loggers).to include("root_logger")
    end
  end
  context "when #logger is called" do
    subject { TestClass.new() }
    it "creates a new logger in LogHelper::LogConfig" do
      LogHelper::LogConfig.init()

      logger = subject.logger()
      expect(logger).to be_a_kind_of(Logging::Logger)
      expect(LogHelper::LogConfig.loggers).to include("TestClass")
    end
  end
end
