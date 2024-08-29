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

  def max_query(table_name: nil, pks: nil)
    %(SELECT max(#{pks.column_name}) AS PK_MAX FROM `#{table_name}`)
  end

  def min_query(table_name: nil, pks: nil)
    %(SELECT min(#{pks.column_name}) AS PK_MIN FROM `#{table_name}`)
  end

  def row_count_query(table_name: nil, pks: nil)
    %(SELECT count(#{pks.column_name}) AS ROW_COUNT FROM `#{table_name}`)
  end

  def chunk_checksum_query_bounded(table_name: nil, pks: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    @logger.debug("Primary key for #{table_name}: #{pks.column_name}")

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT #{pks.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM `#{table_name}`
          WHERE #{pks.column_name} >= ? AND #{pks.column_name} <= ?
      ) as t;
    QUERY
    @logger.debug("bounded checksum query: #{query}")

    query
  end

  def chunk_checksum_query_unbounded(table_name: nil, pks: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT #{pks.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM `#{table_name}`
          WHERE #{pks.column_name} >= ?
          LIMIT ?
      ) as t;
    QUERY
    @logger.debug("unbounded checksum query: #{query}")

    query
  end

  def row_checksum_query(table_name: nil, pks: nil, columns: [])
    col_str = columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(",\n")

    query = <<~QUERY
      SELECT #{pks.column_name} AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
      FROM `#{table_name}`
      WHERE #{pks.column_name} >= ? AND #{pks.column_name} <= ?
      ORDER BY #{pks.column_name}
    QUERY

    @logger.debug("generated row checksum query: #{query}")

    query
  end

  def select_all_raw_query(table_name: nil, pks: nil, row_id: nil)
    query = %(select * from `#{table_name}` where #{pks.column_name} = #{row_id})
    @logger.debug("select all raw query: #{query}")

    query
  end

  def select_all_query(table_name: nil, pks: nil)
    query = %(select * from `#{table_name}` where #{pks.column_name} = ?)
    @logger.debug("select all query: #{query}")

    query
  end

end
