require 'factory_bot'

CHUNK_SIZE = 112
RANDOM = Random.new

FactoryBot.define do
  sequence :min_id do |n|
    "#{12 + n * CHUNK_SIZE}"
  end

  sequence :max_id do |n|
    "#{134 + n * CHUNK_SIZE}"
  end

  factory :row_checksum, class: 'RowChecksum' do
    transient do
      name { 'addresses' }
      pk { 'id' }
    end
    row_id { RANDOM.rand(1..13_000) }
    crc32 { RANDOM.rand(1_000_000..999_999_999) }
    primary_key { pk }
    table_name { name }
  end

  factory :chunk_checksum, class: 'ChunkChecksum' do
    transient do
      name { 'addresses' }
      pk { 'id' }
    end
    min { generate(:min_id) }
    max { generate(:max_id) }
    count { max.to_i - min.to_i }
    crc32 { RANDOM.rand(1_000_000..999_999_999) }
    primary_key { pk }
    table_name { name }
  end

  factory :chunk_comparison, class: 'ChunkComparison' do
    transient do
      name { 'addresses' }
      min { generate(:min_id) }
      max { generate(:max_id) }
      count { max.to_i - min.to_i }
      crc32 { RANDOM.rand(1_000_000..999_999_999) }
    end
    master { build(:chunk_checksum, table_name: name, min: min, max: max, count: count) }
    replica { build(:chunk_checksum, table_name: name, min: min, max: max, count: count) }
  end

  factory :row_comparison, class: 'RowComparison' do
    transient do
      row_id { generate(:min_id) }
      crc32 { RANDOM.rand(1_000_000..999_999_999) }
    end
    master { build(:row_checksum, row_id: row_id) }
    replica { build(:row_checksum, row_id: row_id) }
  end
end
