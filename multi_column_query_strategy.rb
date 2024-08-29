# frozen_string_literal: true

require_relative 'log_helper'
require_relative 'multi_column_primary_key'
require_relative 'query_strategy'

# Caveat Emptor: The multi-column primary key is an edge case, so we're not going to try too hard to optimize the SQL logic for this use case.
#
class MultiColumnQueryStrategy < QueryStrategy
  include LogHelper

  def initialize(opts = {})
    super
  end

  def row_id_query(table_name: nil, pks: nil)
    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")

    row_id_query = pks.columns.map do |col|
      %{COALESCE(`#{table_name}`.#{col}, "")}
    end.join(%(, "::", ))

    %(CONCAT(#{row_id_query}))
  end

  def max_query(table_name: nil, pks: nil)
    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    # %(SELECT max(#{row_id_query}) AS PK_MAX FROM `#{table_name}`)

    query = <<~QUERY
      SELECT max(t.ROW_ID) AS PK_MAX
      FROM (
        SELECT #{row_id_query} as ROW_ID
        FROM `#{table_name}`
      ) as t
    QUERY

    @logger.debug("max query: #{query}")
    query
  end

  def min_query(table_name: nil, pks: nil)
    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    # %(SELECT min(#{row_id_query}) AS PK_MIN FROM `#{table_name}`)

    query = <<~QUERY
      SELECT min(t.ROW_ID) AS PK_MIN
      FROM (
        SELECT #{row_id_query} as ROW_ID
        FROM `#{table_name}`
      ) as t
    QUERY

    @logger.debug("min query: #{query}")
    query
  end

  def row_count_query(table_name: nil, pks: nil)
    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    # %(SELECT count(#{row_id_query}) AS ROW_COUNT FROM `#{table_name}`)

    query = <<~QUERY
      SELECT count(t.ROW_ID) AS ROW_COUNT
      FROM (
        SELECT #{row_id_query} as ROW_ID
        FROM `#{table_name}`
      ) as t
    QUERY

    @logger.debug("row count query: #{query}")
    query
  end

  def chunk_checksum_query_bounded(table_name: nil, pks: nil, columns: nil)
    col_str = columns.map do |col|
      %{COALESCE(tt.#{col}, "")}
    end.join(",\n")

    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM (
               SELECT #{row_id_query} AS ROW_ID, #{columns.join(", ")}
               FROM `#{table_name}`
               ORDER BY #{pks.columns.join(", ")}
          ) as tt
          WHERE tt.ROW_ID >= ? AND tt.ROW_ID <= ?
      ) as t;
    QUERY
    @logger.debug("bounded checksum query: #{query}")

    query
  end

  def chunk_checksum_query_unbounded(table_name: nil, pks: nil, columns: nil)
    col_str = columns.map do |col|
      %{COALESCE(tt.#{col}, "")}
    end.join(",\n")

    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    query = <<~QUERY
      SELECT COALESCE(min(t.ROW_ID), "") as START,
        COALESCE(max(t.ROW_ID), "") as END,
        COALESCE(count(t.ROW_ID), 0) as COUNT,
        COALESCE(CRC32(group_concat(t.CHECKSUM separator "|")), 0) as CHECKSUM

      FROM(
          SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
          FROM (
               SELECT #{row_id_query} AS ROW_ID, #{columns.join(", ")}
               FROM `#{table_name}`
               ORDER BY #{pks.columns.join(", ")}
          ) as tt
      WHERE tt.ROW_ID >= ?
      LIMIT ?
      ) as t;
    QUERY
    @logger.debug("unbounded checksum query: #{query}")

    query
  end

  def row_checksum_query(table_name: nil, pks: nil, columns: nil)
    col_str = columns.map do |col|
      %{COALESCE(tt.#{col}, "")}
    end.join(",\n")

    @logger.debug("Primary key for #{table_name}: #{pks.columns.inspect}")
    row_id_query = row_id_query(table_name: table_name, pks: pks)

    query = <<~QUERY
      SELECT tt.ROW_ID AS ROW_ID, CRC32(CONCAT(#{col_str})) as CHECKSUM
      FROM (
           SELECT #{row_id_query} AS ROW_ID, #{columns.join(", ")}
           FROM `#{table_name}`
           ORDER BY #{pks.columns.join(", ")}
      ) as tt
      WHERE ROW_ID >= ? AND ROW_ID <= ?
      ORDER BY tt.ROW_ID
    QUERY

    @logger.debug("generated row checksum query for table #{table_name}: #{query}")

    query
  end

  def select_all_raw_query(table_name: nil, pks: nil, row_id: nil)
    values = row_id.split(/::/)
    pairs = []
    pks.columns.each_with_index do |key, index|
      pairs.push(%(#{key} = #{values[index]}))
    end

    query = %(select * from `#{table_name}` where #{pairs.join(" AND ")})
    @logger.debug("select all raw query: #{query}")

    query
  end

  def select_all_query(table_name: nil, pks: nil)
    pairs = pks.columns.map do |col|
      %(#{col} = ?)
    end.join(" AND ")

    query = %(select * from `#{table_name}` where #{pairs})
    @logger.debug("select all query: #{query}")

    query
  end
end
