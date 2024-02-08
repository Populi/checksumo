require "rspec"
require "logging"
require "mysql2"

require_relative "../executor"

describe Executor do
  let(:logger) { double(Logging::Logger) }
  let(:mysql_client) { double(Mysql2::Client) }
  describe "#new" do
    context "when no optional arguments are given" do
      subject { Executor.new(logger: logger) }
      it "has the default value for @retry_count" do
        expect(subject.retry_count).to eq(Executor::DEFAULT_RETRY_COUNT)
      end
      it "has the default value for @retry_wait" do
        expect(subject.retry_wait).to eq(Executor::DEFAULT_RETRY_WAIT)
      end
    end
    context "when optional arguments are given" do
      retry_wait = 172654
      retry_count = 10000000
      subject { Executor.new(retry_count: retry_count, retry_wait: retry_wait, logger: logger) }
      it "has the assigned value for @retry_count" do
        expect(subject.retry_count).to eq(retry_count)
      end
      it "has the assigned value for @retry_wait" do
        expect(subject.retry_wait).to eq(retry_wait)
      end
    end
  end

  describe "#execute" do
    retry_wait = 1
    retry_count = 3
    subject { Executor.new(retry_count: retry_count, retry_wait: retry_wait, logger: logger) }
    before do
      allow(logger).to receive(:debug)
      allow(logger).to receive(:info)
      allow(logger).to receive(:warn)
      allow(logger).to receive(:error)
    end
    context "when block raises error on each call" do
      it "should raise the Exception after on initial attempt and retry_count retry attempts" do
        total_attempts = retry_count + 1
        expect(mysql_client).to receive(:query).exactly(total_attempts).times { raise "some spurious sql-related error" }
        expect do
          subject.execute do
            mysql_client.query("select * from should_raise_error;")
          end
        end.to raise_error(RuntimeError)
      end
    end
    context "when block succeeds on the second retry" do
      it "should return the value of the block" do
        expect(mysql_client).to receive(:query).twice.and_invoke(lambda { |q| raise "some spurious sql-related error" }, lambda { |q| Hash[] })

        result = subject.execute do
          mysql_client.query("select * from should_raise_error;")
        end
        expect(result).to eq(Hash[])
      end
    end
    context "when the returned error is on the no-retry list" do
      subject {
        Executor.new(retry_count: retry_count,
          retry_wait: retry_wait,
          logger: logger,
          no_retry: [Mysql2::Error, ZeroDivisionError])
      }
      it "should raise the Exception after with no retries" do
        expect(mysql_client).to receive(:query).once { raise ZeroDivisionError.new("some spurious sql-related error") }
        expect do
          subject.execute do
            mysql_client.query("select * from should_raise_error;")
          end
        end.to raise_error(ZeroDivisionError)
      end
    end
    context "when an on-fail is provided" do
      subject {
        Executor.new(retry_count: retry_count,
          retry_wait: retry_wait,
          logger: logger,
          no_retry: [Mysql2::Error, ZeroDivisionError])
      }
      it "should return the value of the on-fail method" do
        expect(mysql_client).to receive(:query).once { raise ZeroDivisionError.new("some spurious sql-related error") }
        fallback = ->(_e) { "how do you like them apples?" }

        result = subject.execute(on_fail: fallback) do
          mysql_client.query("select * from should_raise_error;")
        end

        expect(result).to match(/how do you like them apples\?/)
      end
    end
  end
end
