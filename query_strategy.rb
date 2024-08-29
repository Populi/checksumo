# frozen_string_literal: true

require_relative 'log_helper'

class QueryStrategy
  include LogHelper

  def initialize(opts = {})
    @logger = opts.fetch(:logger) do
      logger
    end
  end

  def generate_delete(table_name: nil, row_id: nil) end

  def max_query(table_name: nil, primary_key: nil)
    %(SELECT max(#{primary_key.column_name}) AS PK_MAX FROM `#{table_name}`)
  end

  def min_query(table_name: nil, primary_key: nil)
    %(SELECT min(#{primary_key.column_name}) AS PK_MIN FROM `#{table_name}`)
  end

  def row_count_query(table_name: nil, primary_key: nil)
    %(SELECT count(#{primary_key.column_name}) AS ROW_COUNT FROM `#{table_name}`)
  end

  def chunk_checksum_query_bounded(table_name: nil, primary_key: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    @logger.debug("Primary key for #{table_name}: #{primary_key.column_name}")

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT #{primary_key.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM `#{table_name}`
          WHERE #{primary_key.column_name} >= ? AND #{primary_key.column_name} <= ?
      ) as t;
    QUERY
    @logger.debug("bounded checksum query: #{query}")

    query
  end

  def chunk_checksum_query_unbounded(table_name: nil, primary_key: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT #{primary_key.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM `#{table_name}`
          WHERE #{primary_key.column_name} >= ?
          LIMIT ?
      ) as t;
    QUERY
    @logger.debug("unbounded checksum query: #{query}")

    query
  end

  def row_checksum_query(table_name: nil, primary_key: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    query = <<~QUERY
      SELECT #{primary_key.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
      FROM `#{table_name}`
      WHERE #{primary_key.column_name} >= ? AND #{primary_key.column_name} <= ?
      ORDER BY #{primary_key.column_name}
    QUERY
    @logger.debug("generated row checksum query: #{query}")

    query
  end

  def select_all_raw_query(table_name: nil, primary_key: nil, row_id: nil)
    query = %(SELECT * FROM `#{table_name}` WHERE #{primary_key.column_name} = #{row_id})
    @logger.debug("select all raw query: #{query}")

    query
  end

  def select_all_query(table_name: nil, primary_key: nil)
    query = %(SELECT * FROM `#{table_name}` WHERE #{primary_key.column_name} = ?)
    @logger.debug("select all query: #{query}")

    query
  end

end
