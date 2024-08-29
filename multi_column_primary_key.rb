# frozen_string_literal: true

require_relative 'log_helper'
require_relative 'primary_key'

class MultiColumnPrimaryKey < PrimaryKey
  include LogHelper

  attr_accessor :columns

  def initialize(opts = {})
    super
    @columns = if @column_name.include?('::')
                 @column_name.split(/::/)
               else
                 @column_name.split(/,\s*/)
               end
  end

end
