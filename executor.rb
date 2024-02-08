require_relative "log_helper"

# Executor
#
# Execute a block of code, retrying in case of failure.
# @example
#   executor = Executor.new()
#   result = executor.execute(on_fail: proc{|err| err}) do
#     1 / 0
#   end
#   => ZeroDivisionError
#
class Executor
  include LogHelper

  DEFAULT_RETRY_COUNT = 5
  DEFAULT_RETRY_WAIT = 2
  DEFAULT_NO_RETRY_ERRORS = []

  attr_reader :retry_count, :retry_wait

  # Create new Executor
  # @param [Hash] opts the options to create the Executor
  # @option opts [Array<Class>] :no_retry Array of Error classes not to retry
  # @option opts [Integer] :retry_count Number of retries before failing
  # @option opts [Number] :retry_wait Number of seconds to wait before retrying
  # @option opts [Logger] :logger Logger
  def initialize(opts = {})
    @no_retry = opts.fetch(:no_retry, DEFAULT_NO_RETRY_ERRORS)
    @retry_count = opts.fetch(:retry_count, DEFAULT_RETRY_COUNT).to_i
    @retry_wait = opts.fetch(:retry_wait, DEFAULT_RETRY_WAIT).to_f
    @logger = opts.fetch(:logger) { logger }
    @random = Random.new
  end

  # Execute a block synchronously, returning the value of the block evaluation.
  # Retry failures with a naive jitter back-off.
  #
  # @param [Hash] opts the options to use in execution
  # @option opts [Integer] :retry_count Number of retries before failing
  # @option opts [Number] :retry_wait Number of seconds to wait before retrying
  # @option opts [Proc<Error>] :on_fail Proc to execute in case of failure.
  #                                     Proc must accept a single (Error) argument.
  #
  # If :on_fail is not provided, the error from the final failure is raised.
  # If :on_fail is provided, it will be executed on final failure, with the final error as its sole argument.
  #
  def execute(opts = {}, &block)
    retry_count = opts.fetch(:retry_count, @retry_count)
    retry_wait = opts.fetch(:retry_wait, @retry_wait)
    on_fail = opts.fetch(:on_fail, proc { |err| raise err })

    begin
      yield if block
    rescue => err
      @logger.error("caught error #{err}")
      return on_fail.call(err) if @no_retry.include? err.class

      @logger.debug("retrying...")
      return on_fail.call(err) unless retry_count.positive?

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
