# frozen_string_literal: true

require_relative 'log_helper'

# Strategy to generate Primary Key clauses in SQL queries for each Table.
# The simplest case is a single-column Primary Key.
class PrimaryKey
  include LogHelper

  PRIMARY_KEY_NAME = "ROW_ID"

  attr_accessor :column_name, :primary_key_name, :table_name

  def initialize(opts = {})
    @column_name = opts.fetch(:column_name) do
      raise "Cannot create a PrimaryKeyStrategy without a column name"
    end
    @logger = opts.fetch(:logger) do
      logger
    end
    @primary_key_name = opts.fetch(:primary_key_name) do
      PRIMARY_KEY_NAME
    end
    @table_name = opts.fetch(:table_name) do
      raise "Cannot create a PrimaryKeyStrategy without a table name"
    end
  end
end


