# frozen_string_literal: true

require 'rspec'
require_relative '../log_helper'
require_relative '../primary_key'

RSpec.describe 'PrimaryKeyStrategy' do
  describe "constructor" do
    context 'when column is not supplied' do
      it 'should raise an exception' do
        expect {
          _instance = PrimaryKey.new()
        }.to raise_exception()
      end
    end
    context 'when table_name is not supplied' do
      it 'should raise an exception' do
        expect {
          _instance = PrimaryKey.new(column_name: "some column name")
        }.to raise_exception()
      end
    end
    context "when required fields are provided" do
      let(:column_name) { "id" }
      let(:table_name) { SecureRandom.uuid }
      subject { PrimaryKey.new(column_name: column_name, table_name: table_name) }

      it "should provide a working Logger" do
        expect(subject.logger).to_not be_nil
        expect(subject.logger).to be_a(Logging::Logger)
      end
    end
  end
end

