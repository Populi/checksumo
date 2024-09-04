# frozen_string_literal: true

require 'rspec'
require 'logging'
require_relative '../primary_key'
require_relative '../query_strategy'

RSpec.describe 'QueryStrategy' do
  describe "#new" do
    subject { QueryStrategy.new() }

    it "should have a valid logger" do
      expect(subject.logger).to_not be_nil
      expect(subject.logger).to be_a(Logging::Logger)
    end
  end
  describe "#max_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT max(id) AS PK_MAX FROM `#{table_name}`
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.max_query(table_name: table_name, primary_key: primary_key).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#min_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT min(id) AS PK_MIN FROM `#{table_name}`
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.min_query(table_name: table_name, primary_key: primary_key).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#row_count_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT count(id) AS ROW_COUNT FROM `#{table_name}`
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.row_count_query(table_name: table_name, primary_key: primary_key).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#chunk_checksum_query_bounded" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:columns) { %w[first_name, last_name, middle_initial] }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM
        FROM( 
            SELECT id AS ROW_ID,
                   CRC32(CONCAT(COALESCE(`#{table_name}`.first_name,, ""), COALESCE(`#{table_name}`.last_name,, ""), COALESCE(`#{table_name}`.middle_initial, ""))) as CHECKSUM
            FROM `#{table_name}`
            WHERE id >= ? AND id <= ? 
        ) as t;
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.chunk_checksum_query_bounded(table_name: table_name, primary_key: primary_key, columns: columns).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#chunk_checksum_query_unbounded" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:columns) { %w[first_name, last_name, middle_initial] }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT COALESCE(min(t.ROW_ID), "") as START,
               COALESCE(max(t.ROW_ID), "") as END,
               COALESCE(count(t.ROW_ID), 0) as COUNT,
               COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM
        FROM( 
            SELECT id AS ROW_ID,
                   CRC32(CONCAT(COALESCE(`#{table_name}`.first_name,, ""), COALESCE(`#{table_name}`.last_name,, ""), COALESCE(`#{table_name}`.middle_initial, ""))) as CHECKSUM
            FROM `#{table_name}`
            WHERE id >= ?
            LIMIT ?
        ) as t;
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.chunk_checksum_query_unbounded(table_name: table_name, primary_key: primary_key, columns: columns).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#row_checksum_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:columns) { %w[first_name, last_name, middle_initial] }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT id AS ROW_ID, 
               CRC32(CONCAT(COALESCE(`#{table_name}`.first_name,, ""), COALESCE(`#{table_name}`.last_name,, ""), COALESCE(`#{table_name}`.middle_initial, ""))) as CHECKSUM
        FROM `#{table_name}`
        WHERE id >= ? AND id <= ?
        ORDER BY id
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.row_checksum_query(table_name: table_name, primary_key: primary_key, columns: columns).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#select_all_raw_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:row_id) { SecureRandom.uuid }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT * FROM `#{table_name}` WHERE id = #{row_id}
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.select_all_raw_query(table_name: table_name, primary_key: primary_key, row_id: row_id).gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#select_all_query" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    it "should generate the correct query" do
      expected_query = <<~QUERY
        SELECT * FROM `#{table_name}` WHERE id = ?
      QUERY
      expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

      query = subject.select_all_query(table_name: table_name, primary_key: primary_key) #.gsub(/\s+/, " ").gsub(/\s*$/, "")
      expect(query).to eql(expected_query)
    end
  end
  describe "#where_clause" do
    let(:table_name) { SecureRandom.uuid }
    let(:column_name) { 'id' }
    let(:primary_key) { PrimaryKey.new(column_name: column_name, table_name: table_name) }

    subject { QueryStrategy.new() }

    context "when :row_id is not null" do
      let(:row_id) { SecureRandom.uuid }

      it "should generate the correct query" do
        expected_query = <<~QUERY
          WHERE id = #{row_id}
        QUERY
        expected_query.gsub!(/\s+/, " ").gsub!(/\s*$/, "")

        query = subject.where_clause(table_name: table_name, primary_key: primary_key, row_id: row_id)
        expect(query).to eql(expected_query)
      end
    end
    context "when :row_id is null" do
      it "should generate the correct query" do
        expected_query = <<~QUERY
          WHERE id IS NULL
        QUERY

        query = subject.where_clause(table_name: table_name, primary_key: primary_key, row_id: nil)
        expect(query).to eql(expected_query.gsub(/\s+/, " ").gsub(/\s*$/, ""))
      end
    end
  end
end
