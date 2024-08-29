# frozen_string_literal: true

require 'rspec'
require_relative '../log_helper'
require_relative '../multi_column_primary_key'

RSpec.describe 'MultiColumnPrimaryKeyStrategy' do
  describe '#new' do
    context 'when column is not supplied' do
      it 'should raise an exception' do
        expect {
          _instance = MultiColumnPrimaryKey.new()
        }.to raise_exception(/without a column name/)
      end
    end
    context 'when table_name is not supplied' do
      it 'should raise an exception' do
        expect {
          _instance = MultiColumnPrimaryKey.new(column_name: "some column name")
        }.to raise_exception(/without a table name/)
      end
    end
  end
  context "when the necessary values are provided" do
    let(:table_name) { SecureRandom.uuid }
    let(:columns) { %w[id org_id country_code] }

    context "when column_name is delimited by '::'" do
      let(:column_name) { columns.join("::") }
      subject { MultiColumnPrimaryKey.new(column_name: column_name, table_name: table_name) }

      it "should initialize the class correctly" do
        expect(subject.columns).to eq(columns)
      end
      it "should assign logger correctly" do
        expect(subject.logger).to_not be_nil
      end
    end
    context "when column_name is delimited by ','" do
      let(:column_name) { columns.join(",") }
      subject { MultiColumnPrimaryKey.new(column_name: column_name, table_name: table_name) }

      it "should initialize the class correctly" do
        expect(subject.columns).to eq(columns)
      end
      it "should assign logger correctly" do
        expect(subject.logger).to_not be_nil
      end
    end
    context "when column_name is delimited by ', '" do
      let(:column_name) { columns.join(", ") }
      subject { MultiColumnPrimaryKey.new(column_name: column_name, table_name: table_name) }

      it "should initialize the class correctly" do
        expect(subject.columns).to eq(columns)
      end
      it "should assign logger correctly" do
        expect(subject.logger).to_not be_nil
      end
    end
  end
end
