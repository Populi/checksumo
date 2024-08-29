# frozen_string_literal: true

require 'rspec'
require_relative '../multi_column_query_strategy'

RSpec.describe 'MultiColumnQueryStrategy' do
  describe "#new" do
    subject { MultiColumnQueryStrategy.new() }

    it "should have a valid logger" do
      expect(subject.logger).to_not be_nil
      expect(subject.logger).to be_a(Logging::Logger)
    end
  end
  describe "#row_id_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "should generate a working row_id query" do
      query = subject.row_id_query(table_name: table_name, primary_key: pk)
      expect(query).to match(/COALESCE\(`#{table_name}`/)
    end
  end
  describe "#max_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      query = subject.max_query(table_name: table_name, primary_key: pk)
      expect(query).to match(/SELECT max\(t\.ROW_ID\) AS PK_MAX/)
    end
  end
  describe "#min_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      query = subject.min_query(table_name: table_name, primary_key: pk)
      expect(query).to match(/SELECT min\(t\.ROW_ID\) AS PK_MIN/)
    end
  end
  describe "#row_count_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      query = subject.row_count_query(table_name: table_name, primary_key: pk)
      expect(query).to match(/SELECT count\(t\.ROW_ID\) AS ROW_COUNT/)
    end
  end
  describe "#chunk_checksum_query_bounded" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:columns) { %w[some_column, some_other_column] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

        FROM(
            SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(COALESCE(tt.some_column,, ""),
                                                     COALESCE(tt.some_other_column, ""))) as CHECKSUM
            FROM (
                 SELECT CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS ROW_ID, some_column,, some_other_column
                 FROM `#{table_name}`
                 ORDER BY id, org_id, country_code
            ) as tt
        WHERE tt.ROW_ID >= ? AND tt.ROW_ID <= ?
        ) as t;
      EXPECTED_QUERY

      # The formatting isn't something I'm willing to fight about, so we're going to try and squash whitespace
      expected_query.gsub!(/\s+/, " ")

      query = subject.chunk_checksum_query_bounded(table_name: table_name, primary_key: pk, columns: columns)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query)
    end
  end
  describe "#chunk_checksum_query_unbounded" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:columns) { %w[some_column, some_other_column] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

        FROM(
            SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(COALESCE(tt.some_column,, ""),
                                                     COALESCE(tt.some_other_column, ""))) as CHECKSUM
            FROM (
                 SELECT CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS ROW_ID, some_column,, some_other_column
                 FROM `#{table_name}`
                 ORDER BY id, org_id, country_code
            ) as tt
        WHERE tt.ROW_ID >= ?
        LIMIT ?
        ) as t;
      EXPECTED_QUERY

      # The formatting isn't something I'm willing to fight about, so we're going to try and squash whitespace
      expected_query.gsub!(/\s+/, " ")

      query = subject.chunk_checksum_query_unbounded(table_name: table_name, primary_key: pk, columns: columns)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query)
    end
  end
  describe "#row_checksum_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:columns) { %w[some_column some_other_column] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(COALESCE(tt.some_column, ""),
                                                 COALESCE(tt.some_other_column, ""))) as CHECKSUM
        FROM (
             SELECT CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS ROW_ID, some_column, some_other_column
             FROM `#{table_name}`
             ORDER BY id, org_id, country_code
        ) as tt
        WHERE ROW_ID >= ? AND ROW_ID <= ?
        ORDER BY tt.ROW_ID
      EXPECTED_QUERY

      # The formatting isn't something I'm willing to fight about, so we're going to try and squash whitespace
      expected_query.gsub!(/\s+/, " ")

      query = subject.row_checksum_query(table_name: table_name, primary_key: pk, columns: columns)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query)
    end
  end
  describe "#select_all_raw_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:row_id) { '12345::678::us' }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT * FROM `#{table_name}` WHERE id = 12345 AND org_id = 678 AND country_code = us
      EXPECTED_QUERY

      # The formatting isn't something I'm willing to fight about, so we're going to try and squash whitespace
      expected_query.gsub!(/\s+/, " ")
      expected_query.gsub!(/\s*$/, "")

      query = subject.select_all_raw_query(table_name: table_name, primary_key: pk, row_id: row_id)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query)
    end
  end
  describe "#select_all_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT * FROM `#{table_name}` WHERE id = ? AND org_id = ? AND country_code = ?
      EXPECTED_QUERY

      # The formatting isn't something I'm willing to fight about, so we're going to try and squash whitespace
      expected_query.gsub!(/\s+/, " ")
      expected_query.gsub!(/\s*$/, "")

      query = subject.select_all_query(table_name: table_name, primary_key: pk)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query)
    end
  end
end
