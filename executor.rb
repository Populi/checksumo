require_relative "log_helper"

# Create centralized location for execution, including retry logic.
class Executor
  include LogHelper

  DEFAULT_RETRY_COUNT = 5
  DEFAULT_RETRY_WAIT = 2
  DEFAULT_NO_RETRY_ERRORS = []

  attr_reader :retry_count, :retry_wait

  def initialize(opts = {})
    @no_retry = opts.fetch(:no_retry, DEFAULT_NO_RETRY_ERRORS)
    @retry_count = opts.fetch(:retry_count, DEFAULT_RETRY_COUNT).to_i
    @retry_wait = opts.fetch(:retry_wait, DEFAULT_RETRY_WAIT).to_f
    @logger = opts.fetch(:logger) { logger }
    @random = Random.new
  end

  # Execute a block synchronously
  # Retries failures with a jitter back-off
  def execute(opts = {}, &block)
    retry_count = opts.fetch(:retry_count, @retry_count)
    retry_wait = opts.fetch(:retry_wait, @retry_wait)

    begin
      yield if block
    rescue => err
      @logger.error("caught error #{err}")
      raise err if @no_retry.include? err.class

      @logger.debug("retrying...")
      raise err unless retry_count.positive?

      retry_count -= 1
      @logger.debug("#{retry_count} retries left")

      wait = @random.rand(0..retry_wait)
      retry_wait = 2 * retry_wait

      @logger.debug("sleeping #{retry_wait}s before trying again")
      sleep(wait)
      retry
    end
  end
end
