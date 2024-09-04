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
  describe "#padded_row_id_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "should generate a working row_id query" do
      expected_query = %{CONCAT(LPAD(COALESCE(`#{table_name}`.id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.org_id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.country_code, ""), 16, 0))}
      query = subject.padded_row_id_query(table_name: table_name, primary_key: pk)
      expect(query).to eql(expected_query)
    end
  end
  describe "#row_id_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "should generate a working row_id query" do
      expected_query = %{CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, ""))}
      query = subject.row_id_query(table_name: table_name, primary_key: pk)
      expect(query).to eql(expected_query)
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
    let(:columns) { %w[some_column some_other_column] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(min(t.VISIBLE_ROW_ID), "") as VISIBLE_START,
               COALESCE(max(t.VISIBLE_ROW_ID), "") as VISIBLE_END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM
        FROM(
            SELECT tt.ROW_ID AS ROW_ID,
                   tt.VISIBLE_ROW_ID AS VISIBLE_ROW_ID,
                   CRC32(CONCAT(COALESCE(tt.some_column, ""), COALESCE(tt.some_other_column, ""))) as CHECKSUM
            FROM (
                 SELECT CONCAT(LPAD(COALESCE(`#{table_name}`.id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.org_id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.country_code, ""), 16, 0)) AS ROW_ID,
                        CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS VISIBLE_ROW_ID,
                        some_column,
                        some_other_column 
                 FROM `#{table_name}`          
                 ORDER BY id, org_id, country_code     
            ) as tt
            WHERE tt.ROW_ID >= ? AND tt.ROW_ID <= ?
        ) as t; 
      EXPECTED_QUERY

      query = subject.chunk_checksum_query_bounded(table_name: table_name, primary_key: pk, columns: columns)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query.gsub(/\s+/, " "))
    end
  end
  describe "#chunk_checksum_query_unbounded" do
    let(:table_name) { SecureRandom.uuid }
    let(:pk_columns) { %w[id org_id country_code] }
    let(:columns) { %w[some_column some_other_column] }
    let(:pk) { MultiColumnPrimaryKey.new(column_name: pk_columns.join('::'), table_name: table_name) }

    subject { MultiColumnQueryStrategy.new }

    it "generates the correct SQL query" do
      expected_query = <<~EXPECTED_QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(min(t.VISIBLE_ROW_ID), "") as VISIBLE_START,
               COALESCE(max(t.VISIBLE_ROW_ID), "") as VISIBLE_END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM
        FROM(     
            SELECT tt.ROW_ID AS ROW_ID,
                   tt.VISIBLE_ROW_ID AS VISIBLE_ROW_ID,
                   CRC32(CONCAT(COALESCE(tt.some_column, ""), COALESCE(tt.some_other_column, ""))) as CHECKSUM
            FROM (          
                 SELECT CONCAT(LPAD(COALESCE(`#{table_name}`.id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.org_id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.country_code, ""), 16, 0)) AS ROW_ID,
                        CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS VISIBLE_ROW_ID,
                        some_column,
                        some_other_column
                 FROM `#{table_name}`
                 ORDER BY id, org_id, country_code
            ) as tt
            WHERE tt.ROW_ID >= ?
            LIMIT ?
        ) as t; 
      EXPECTED_QUERY

      query = subject.chunk_checksum_query_unbounded(table_name: table_name, primary_key: pk, columns: columns)
      expect(query.gsub(/\s+/, " ")).to eql(expected_query.gsub(/\s+/, " "))
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
        SELECT tt.ROW_ID AS ROW_ID,
               tt.VISIBLE_ROW_ID AS VISIBLE_ROW_ID, 
               CRC32(CONCAT(COALESCE(tt.some_column, ""), COALESCE(tt.some_other_column, ""))) as CHECKSUM
        FROM (
             SELECT CONCAT(LPAD(COALESCE(`#{table_name}`.id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.org_id, ""), 16, 0), "::", LPAD(COALESCE(`#{table_name}`.country_code, ""), 16, 0)) AS ROW_ID,
                    CONCAT(COALESCE(`#{table_name}`.id, ""), "::", COALESCE(`#{table_name}`.org_id, ""), "::", COALESCE(`#{table_name}`.country_code, "")) AS VISIBLE_ROW_ID,
                    some_column,
                    some_other_column
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
  describe "#where_clause" do
    let(:table_name) { SecureRandom.uuid }
    let(:columns) { %w[id org_id country_code] }
    let(:column_name) { columns.join("::") }
    let(:primary_key) { MultiColumnPrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { MultiColumnQueryStrategy.new() }

    context "when :row_id is not null" do
      let(:row_id) { %w[123, 7, us] }

      it "should generate the correct query" do
        expected_query = <<~QUERY
          WHERE id = #{row_id[0]} AND org_id = #{row_id[1]} AND country_code = #{row_id[2]}
        QUERY
        expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

        query = subject.where_clause(table_name: table_name, primary_key: primary_key, row_id: row_id.join("::"))
        expect(query).to eql(expected_query)
      end
    end
    context "when :row_id is null" do
      it "should generate the correct query" do
        expected_query = <<~QUERY
          WHERE id IS NULL AND org_id IS NULL AND country_code IS NULL
        QUERY

        query = subject.where_clause(table_name: table_name, primary_key: primary_key, row_id: nil)
        expect(query).to eql(expected_query.gsub(/\s+/, " ").gsub(/\s*$/, ""))
      end
    end
  end
end
