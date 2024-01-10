require "rspec"

require_relative "../mysql_connection"
require_relative "../table"

describe "Table" do
  @table_name = "addresses"
  let(:conn) { double(MysqlConnection) }
  let(:logger) { double(Logging::Logger) }

  before do
    allow(logger).to receive(:info)
    allow(logger).to receive(:debug)
    allow(logger).to receive(:debug?) { true }
  end

  describe "#min_row_id" do
    subject { Table.new(@table_name, conn, logger: logger) }

    it "calls the #MysqlConnection.min_row_id" do
      expect(conn).to receive(:min_row_id) { "12" }
      subject.min_row_id
    end
  end

  describe "#max_row_id" do
    subject { Table.new(@table_name, conn, logger: logger) }

    it "calls the #MysqlConnection.max_row_id" do
      expect(conn).to receive(:max_row_id) { "123456" }
      subject.max_row_id
    end
  end

  describe "#chunk_checksum" do
    subject { Table.new(@table_name, conn, logger: logger) }

    it "calls the #MysqlConnection.chunk_checksum" do
      expect(conn).to receive(:chunk_checksum) { build(:chunk_checksum) }
      subject.chunk_checksum
    end
  end

  describe "#row_checksum" do
    subject { Table.new(@table_name, conn, logger: logger) }

    it "calls the #MysqlConnection.row_checksum" do
      expect(conn).to receive(:row_checksum) { build(:row_checksum) }
      subject.row_checksum
    end
  end
end
