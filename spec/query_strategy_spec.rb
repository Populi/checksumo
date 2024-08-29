# frozen_string_literal: true

require 'rspec'
require 'logging'
require_relative '../query_strategy'

RSpec.describe 'ChecksumQueryStrategy' do
  describe "#new" do
    subject { QueryStrategy.new() }

    it "should have a valid logger" do
      expect(subject.logger).to_not be_nil
      expect(subject.logger).to be_a(Logging::Logger)
    end
  end
end
