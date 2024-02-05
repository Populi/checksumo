require "rspec"

require_relative "../mysql_connection"
require_relative "../table"

describe "TablePair" do
  let(:table_name) { "addresses" }
  let(:mconn) { double(MysqlConnection) }
  let(:rconn) { double(MysqlConnection) }
  let(:logger) { double(Logging::Logger) }

  before do
    allow(logger).to receive(:info)
    allow(logger).to receive(:debug)
    allow(logger).to receive(:debug?) { true }
  end

  describe "methods" do
    subject { TablePair.new(table_name, mconn, rconn, logger: logger) }
    it { should respond_to(:database_name) }
    it { should respond_to(:master) }
    it { should respond_to(:replica) }
    it { should respond_to(:table_name) }
  end

  describe "#new" do
    subject { TablePair.new(table_name, mconn, rconn, logger: logger) }

    it "has the correct table name" do
      expect(subject.table_name).to eq(table_name)
    end
    it "has the correct master table" do
      expect(subject.master).to be_a_kind_of(Table)
    end
    it "has the correct replica connection" do
      expect(subject.replica).to be_a_kind_of(Table)
    end
  end
  describe "#compare_chunks" do
    let(:count) { 15 }
    subject { TablePair.new(table_name, mconn, rconn, chunk_size: 99, logger: logger) }
    it "calls into the master and replica connections" do
      # only called when @logger.debug?
      allow(mconn).to receive(:max_row_id) { "12345" }
      allow(mconn).to receive(:primary_key) { "id" }

      # should always be called
      expect(mconn).to receive(:min_row_id) { "12" }
      expect(mconn).to receive(:chunk_checksum) do |*args|
        opts = args.pop
        # This SHOULD be fixable within FactoryBot, but I haven't figured it out yet.
        final_limit = opts[:limit] - 1
        ccs = build_list(:chunk_checksum, count, table_name: "addresses")
        ccs << build(:chunk_checksum, count: final_limit)
      end
      expect(rconn).to receive(:chunk_checksum).at_least(4) do |*args|
        opts = args.pop
        build_list(:chunk_checksum, 1, table_name: "addresses", min: opts[:min], max: opts[:max])
      end
      subject.compare_chunks
    end
  end
  describe "#compare_rows" do
    let(:min) { 123 }
    let(:max) { 125 }
    subject { TablePair.new(table_name, mconn, rconn, chunk_size: 99, logger: logger) }
    it "calls into both master and replica connections" do
      expect(mconn).to receive(:row_checksum) do
        (min..max).map do |row|
          build(:row_checksum, row_id: row.to_s)
        end
      end
      expect(rconn).to receive(:row_checksum) do
        (min..max).map do |row|
          build(:row_checksum, row_id: row.to_s)
        end
      end

      subject.compare_rows(min: min, max: max)
    end
  end

  describe "#generate_update" do
    let(:table_name) { "addresses" }
    let(:primary_key) { "id" }
    let(:database_name) { "production_database" }
    let(:row_checksum) { build(:row_checksum, primary_key: primary_key, table_name: table_name) }
    before do
      allow(mconn).to receive(:primary_key) { primary_key }
    end
    context "when database_name is set" do
      subject { TablePair.new(table_name, mconn, rconn, database_name: database_name) }

      it "should produce a list of update commands including database name" do
        expect(mconn).to receive(:row_values) do |*args|
          [{
            primary_key => row_checksum.row_id,
            "number" => "4475",
            "street" => "1st St",
            "city" => "Moscow",
            "state" => "Idaho"
          }]
        end
        expect(rconn).to receive(:row_values) do |*args|
          [{
            primary_key => row_checksum.row_id,
            "number" => "475",
            "street" => "1st St",
            "city" => "Moscow",
            "state" => "Idaho"
          }]
        end
        cmd = subject.generate_update(row_checksum.row_id)
        expect(cmd).to match(/production_database\.addresses/)
      end
    end
    context "when database_name is not set" do
      subject { TablePair.new(table_name, mconn, rconn) }

      it "should produce a list of update commands including only the table name" do
        expect(mconn).to receive(:row_values) do |*args|
          [{
            primary_key => row_checksum.row_id,
            "number" => "4475",
            "street" => "1st St",
            "city" => "Moscow",
            "state" => "Idaho"
          }]
        end
        expect(rconn).to receive(:row_values) do |*args|
          [{
            primary_key => row_checksum.row_id,
            "number" => "475",
            "street" => "1st St",
            "city" => "Moscow",
            "state" => "Idaho"
          }]
        end
        cmd = subject.generate_update(row_checksum.row_id)
        expect(cmd).to match(/addresses/)
      end
    end
  end
end
