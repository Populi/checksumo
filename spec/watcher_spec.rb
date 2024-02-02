require "rspec"

require_relative "../watcher"
require_relative "../mysql_connection"

describe ReplicationWatcher do
  let(:master) { double MysqlConnection }
  let(:replica) { double MysqlConnection }
  let(:logger) { double Logging::Logger }

  before do
    allow(logger).to receive(:debug)
    allow(logger).to receive(:info)
    allow(logger).to receive(:warn)
    allow(logger).to receive(:error)
  end

  describe "#initialize" do
    let(:table_names) { %w[addresses media media_encodings] }
    context "when logger is provided" do
      subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }
      it "should assign master correctly" do
        expect(subject.master).to eq(master)
      end
      it "should assign replica correctly" do
        expect(subject.replica).to eq(replica)
      end
      it "should assign logger correctly" do
        expect(subject.logger).to eq(logger)
      end
    end
    context "when logger is not provided" do
      subject { ReplicationWatcher.new(master: master, replica: replica, table_names: table_names) }
      it "should assign master correctly" do
        expect(subject.master).to eq(master)
      end
      it "should assign replica correctly" do
        expect(subject.replica).to eq(replica)
      end
      it "should generate a new logger correctly" do
        logger = subject.logger
        expect(logger).to be_a_kind_of(Logging::Logger)
        expect(logger.name).to eql("#{subject.class.name}_logger")
      end
    end
  end

  describe "#reset" do
    let(:table_names) { %w[addresses media media_encodings] }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }

    context "when no table names are provided" do
      let(:table_names) { %w[notes orders people] }

      it "should replace the existing table_names with an empty list" do
        subject.reset([])
        expect(subject.table_pairs).to be_empty
      end
    end

    context "when a list of table names is provided" do
      let(:table_names) { %w[notes orders people] }

      it "should replace the existing table_names" do
        subject.reset(table_names)
        subject.table_pairs.each do |pair|
          expect(table_names).to include(pair.table_name)
        end
      end
    end
  end

  describe "#search" do
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger) }
    it "calls master.search" do
      expect(master).to receive(:search) do
        {
          "people" => "id",
          "employees" => "id",
          "students" => "id",
          "books" => "isbn, author"
        }
      end
      expect(subject.search).to be_a_kind_of(Hash)
    end
  end

  describe "#delta" do
    let(:table_names) { %w[addresses media media_encodings] }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }
    context "when table_names is empty" do
      it "should return an empty list" do
        subject.reset([])
        delta = subject.delta
        expect(delta).to be_empty
      end
    end
    context "when table_names is not empty" do
      let(:primary_key) { "some_primary_key" }
      let(:min_row_id) { 12 }
      let(:max_row_id) { 32 }
      let(:row_count) { 15 }
      let(:crc32) { 80012233455667 }
      it "should query each table pair for its delta" do
        allow(master).to receive(:primary_key) { primary_key }
        allow(master).to receive(:min_row_id) { min_row_id }
        allow(master).to receive(:max_row_id) { max_row_id }

        allow(replica).to receive(:primary_key) { primary_key } # probably don't need this

        expect(master).to receive(:chunk_checksum).at_least(3).times do |table_name, opts|
          [ChunkChecksum.new(table_name: table_name,
            crc32: crc32,
            min: min_row_id,
            max: max_row_id,
            count: row_count,
            primary_key: primary_key)]
        end

        expect(replica).to receive(:chunk_checksum).at_least(3).times do |table_name, opts|
          [ChunkChecksum.new(table_name: table_name,
            crc32: crc32,
            min: min_row_id,
            max: max_row_id,
            count: row_count,
            primary_key: primary_key)]
        end
        subject.delta do |pair|
          pp pair
        end
      end
    end
  end

  describe "#generate_delete" do
    let(:table_names) { %w[addresses media media_encodings] }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }

    context "when there are rows on both replicas" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", master: master, replica: replica) }

      it "should not return commands" do
        expect(master).to_not receive(:generate_delete)
        expect(subject.generate_delete(row_comparison)).to be_empty
      end
    end

    context "when there is a replica value but no master value" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", replica: replica) }
      context "when master generates no sql commands" do
        before do
          allow(master).to receive(:generate_delete).once { [] }
        end
        it "should return an empty string" do
          expect(subject.generate_delete(row_comparison)).to be_empty
        end
      end

      context "when there are sql commands" do
        let(:primary_key) { "some_primary_key" }
        before do
          allow(master).to receive(:generate_delete) do |table_name, row_id|
            [
              %(DELETE FROM #{table_name} WHERE #{primary_key} = '#{row_id}';)
            ]
          end
        end
        it "should return text" do
          expect(subject.generate_delete(row_comparison)).to be_truthy
        end
      end
    end
  end

  describe "#generate_insert" do
    let(:table_names) { %w[addresses media media_encodings] }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }

    context "when there are rows on both replicas" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", master: master, replica: replica) }

      it "should not return text" do
        expect(master).to_not receive(:generate_insert)
        expect(subject.generate_insert(row_comparison)).to be_empty
      end
    end

    context "when there is a master value but no replica value" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", master: master) }
      context "when master generates no sql commands" do
        before do
          allow(master).to receive(:generate_insert).once { [] }
        end
        it "should not return text" do
          expect(subject.generate_insert(row_comparison)).to be_empty
        end
      end

      context "when there are sql commands" do
        before do
          allow(master).to receive(:generate_insert) do |table_name, _row_id|
            [
              %(INSERT INTO #{table_name} ('SOME_COLUMN', 'SOME_OTHER_COLUMN')
                   VALUES('one value', 'another value');)
            ]
          end
        end
        it "should return text" do
          expect(subject.generate_insert(row_comparison)).to be_truthy
        end
      end
    end
  end

  describe "#generate_update" do
    let(:table_names) { %w[addresses media media_encodings] }
    let(:primary_key) { "id" }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }

    context "when there is a master value but no replica value" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", master: master) }

      it "should return an empty string" do
        expect(master).to_not receive(:generate_update)
        expect(subject.generate_update(row_comparison)).to be_empty
      end
    end

    context "when there are rows on both replicas" do
      let(:row_comparison) { RowComparison.new(row_id: 12, table_name: "addresses", master: master, replica: replica) }
      context "when table pair generates no sql commands" do
        before do
          allow(master).to receive(:primary_key).with(/addresses/) { primary_key }
          allow(master).to receive(:row_values).with(/addresses/, 12).once do |table_name, row_id|
            [{
              primary_key.to_s => row_id,
              "some column" => "some value",
              "some other_column" => "some other value"
            }]
          end
          allow(replica).to receive(:primary_key).with(/addresses/) { primary_key }
          allow(replica).to receive(:row_values).with(/addresses/, 12).once do |table_name, row_id|
            [{
              primary_key.to_s => row_id,
              "some column" => "some value",
              "some other_column" => "some other value"
            }]
          end
        end
        it "should not return text" do
          expect(subject.generate_update(row_comparison)).to be_empty
        end
      end

      context "when there are sql commands" do
        before do
          allow(master).to receive(:primary_key).with(/addresses/) { primary_key }
          allow(master).to receive(:row_values).with(/addresses/, 12).once do |table_name, row_id|
            [{
              primary_key.to_s => row_id,
              "some column" => "some value",
              "some other_column" => "some other value from master"
            }]
          end
          allow(replica).to receive(:primary_key).with(/addresses/) { primary_key }
          allow(replica).to receive(:row_values).with(/addresses/, 12).once do |table_name, row_id|
            [{
              primary_key.to_s => row_id,
              "some column" => "some value",
              "some other_column" => "some other value from replica"
            }]
          end
        end
        it "should return text" do
          expect(subject.generate_update(row_comparison)).to be_truthy
        end
      end
    end
  end

  describe "#reconcile" do
    let(:table_names) { %w[addresses media media_encodings] }
    subject { ReplicationWatcher.new(master: master, replica: replica, logger: logger, table_names: table_names) }
    context "when table_names is empty" do
      it "should return an empty list" do
        subject.reset([])
        delta = subject.delta
        expect(delta).to be_empty
      end
    end
    context "when table_names is not empty" do
      let(:primary_key) { "some_primary_key" }
      let(:min_row_id) { 12 }
      let(:max_row_id) { 32 }
      let(:row_count) { 15 }
      let(:crc32) { 80012233455667 }
      it "should query each table pair for its delta" do
        allow(master).to receive(:primary_key) { primary_key }
        allow(master).to receive(:min_row_id) { min_row_id }
        allow(master).to receive(:max_row_id) { max_row_id }

        allow(replica).to receive(:primary_key) { primary_key } # probably don't need this

        expect(master).to receive(:chunk_checksum).at_least(3).times do |table_name, opts|
          [ChunkChecksum.new(table_name: table_name,
            crc32: crc32,
            min: min_row_id,
            max: max_row_id,
            count: row_count,
            primary_key: primary_key)]
        end

        expect(replica).to receive(:chunk_checksum).at_least(3).times do |table_name, opts|
          [ChunkChecksum.new(table_name: table_name,
            crc32: crc32,
            min: min_row_id,
            max: max_row_id,
            count: row_count,
            primary_key: primary_key)]
        end
        subject.reconcile
      end
    end
  end
end
