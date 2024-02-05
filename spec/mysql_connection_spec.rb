require "rspec"
require "mysql2"

require_relative "../mysql_connection"

describe ChunkChecksum do
  describe "#equal?" do
    subject { build(:chunk_checksum) }
    it "returns false with other is nil" do
      expect(subject.equal?(nil)).to be_falsey
    end
    it "returns true when the fields align" do
      other = ChunkChecksum.new(min: subject.min, max: subject.max, count: subject.count, crc32: subject.crc32)
      expect(subject.equal?(other)).to be_truthy
    end
  end
end

describe RowChecksum do
  describe "#equal?" do
    subject { build(:row_checksum) }
    it "returns false when other is nil" do
      expect(subject.equal?(nil)).to be_falsey
    end
    it "returns true when the fields align" do
      other = RowChecksum.new(row_id: subject.row_id, crc32: subject.crc32)
      expect(subject.equal?(other)).to be_truthy
    end
  end
end

describe MysqlConnection do
  let(:logger) { double(Logging::Logger) }
  let(:mysql_client) { double(Mysql2::Client) }
  let(:database_name) { "production_database_name" }

  before do
    allow(logger).to receive(:debug)
    allow(logger).to receive(:info)
    allow(logger).to receive(:error)
  end

  describe "methods" do
    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    it { should respond_to(:check) }
    it { should respond_to(:search) }
    it { should respond_to(:row_checksum) }
    it { should respond_to(:primary_key) }
    it { should respond_to(:max_row_id) }
    it { should respond_to(:min_row_id) }
    it { should respond_to(:database_name) }
  end

  describe "#new" do
    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    it "has an empty @table_names array" do
      expect(subject.table_names).to be_empty
    end
    it "has an empty @primary_key_cache" do
      expect(subject.primary_key_cache).to be_empty
    end
    context "when database name is provided" do
      it "has the correct database_name" do
        expect(subject.database_name).to eq(database_name)
      end
    end
    context "when database name is NOT provided" do
      subject { MysqlConnection.new(client: mysql_client, logger: logger) }
      it "returns nil" do
        expect(subject.database_name).to be_nil
      end
    end
  end

  describe "#check" do
    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    it "adds table_names to @table_names" do
      tables = %w[addresses aid]
      subject.check(tables)
      expect(subject.table_names).to_not be_empty
    end

    it "adds a single table_name to @table_names" do
      subject.check("some_table_name")
      expect(subject.table_names).to_not be_empty
    end
  end

  describe "#search" do
    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }
    context "when the SQL query succeeds" do
      it "adds results from mysql to the primary_key_cache" do
        expect(mysql_client).to receive(:query) do |_args|
          [{"TableName" => "addresses", "PrimaryKey" => "id"}]
        end

        subject.search

        expect(subject.primary_key_cache).to include(
          {
            "addresses" => "id"
          }
        )
      end
    end

    context "when the search raises a network exception" do
      it "retries the request" do
        expect(mysql_client).to receive(:query).exactly(3).times.and_invoke(lambda { |q| raise Mysql2::ConnectionError.new("test error") },
          lambda { |q| raise Mysql2::TimeoutError.new("test error") },
          lambda { |q| [{"TableName" => "addresses", "PrimaryKey" => "id"}] })
        subject.search
        expect(subject.primary_key_cache).to include(
          {
            "addresses" => "id"
          }
        )
      end
    end
  end

  describe "#columns" do
    let(:result) { double(Mysql2::Result) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    it "executes a simple query" do
      expect(mysql_client).to receive(:query) { result }
      expect(result).to receive(:fields) { %w[id some_field some_other_field another_field] }

      expect(subject.columns("addresses")).to eq(%w[another_field id some_field some_other_field])
    end
  end

  describe "#primary_key" do
    let(:result) { double(Mysql2::Result) }
    let(:statement) { double(Mysql2::Statement) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    it "prepares and executes a query" do
      expect(mysql_client).to receive(:prepare) { statement }
      expect(statement).to receive(:execute) { result }
      expect(result).to receive(:map) { ["id"] }

      expect(subject.primary_key("addresses")).to eq("id")
    end
  end

  describe "#max_row_id" do
    let(:result) { double(Mysql2::Result) }
    let(:statement) { double(Mysql2::Statement) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    context "when primary key cache is empty" do
      it "queries for primary_key before querying for the max value" do
        expect(mysql_client).to receive(:prepare) { statement }
        expect(statement).to receive(:execute) { result }
        expect(result).to receive(:map) { ["id"] }

        expect(mysql_client).to receive(:query) { [{"max" => "12345667"}] }

        expect(subject.max_row_id("addresses")).to eq("12345667")
      end
    end
    context "when primary key cache is populated" do
      it "uses the cached primary_key when querying for the max value" do
        subject.primary_key_cache["addresses"] = "id"
        expect(mysql_client).to receive(:query) { [{"max" => "12345667"}] }
        expect(subject.max_row_id("addresses")).to eq("12345667")
      end
    end
  end

  describe "#min_row_id" do
    let(:result) { double(Mysql2::Result) }
    let(:statement) { double(Mysql2::Statement) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    context "when primary key cache is empty" do
      it "queries for primary_key before querying for the min value" do
        expect(mysql_client).to receive(:prepare) { statement }
        expect(statement).to receive(:execute) { result }
        expect(result).to receive(:map) { ["id"] }

        expect(mysql_client).to receive(:query) { [{"min" => "12"}] }
        expect(subject.min_row_id("addresses")).to eq("12")
      end
    end
    context "when primary key cache is populated" do
      it "uses the cached primary_key when querying for the min value" do
        subject.primary_key_cache["addresses"] = "id"
        expect(mysql_client).to receive(:query) { [{"min" => "12"}] }
        expect(subject.min_row_id("addresses")).to eq("12")
      end
    end
  end

  describe "#row_checksum" do
    let(:checksum_result) { double(Mysql2::Result) }
    let(:col_result) { double(Mysql2::Result) }
    let(:pk_result) { double(Mysql2::Result) }
    let(:checksum_statement) { double(Mysql2::Statement) }
    let(:pk_statement) { double(Mysql2::Statement) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    context "when primary key cache is empty" do
      it "queries for primary_key and columns before querying for the row_checksum" do
        # Primary Key queries
        expect(mysql_client).to receive(:prepare).at_least(:twice) do |*args|
          query = args.first

          if query.match?("CHECKSUM")
            checksum_statement
          elsif query.match?("PRIMARY KEY")
            pk_statement
          end
        end
        expect(pk_statement).to receive(:execute) { pk_result }
        expect(pk_result).to receive(:map) { ["id"] }

        # Columns queries
        expect(mysql_client).to receive(:query) { col_result }
        expect(col_result).to receive(:fields) { %w[id some_field some_other_field another_field] }

        # Checksum queries
        expect(checksum_statement).to receive(:execute) { checksum_result }
        expect(checksum_result).to receive(:map) { build_list(:row_checksum, 1, row_id: "12") }

        checksums = subject.row_checksum("addresses", row_id: "12")
        checksum = checksums.first
        expect(checksum.row_id).to eq("12")
        expect(checksum.crc32).to_not be_nil
      end
    end
    context "when primary key cache is not empty" do
      it "queries for columns before querying for the row_checksum" do
        subject.primary_key_cache["addresses"] = "id"

        # Columns queries
        expect(mysql_client).to receive(:query) { col_result }
        expect(col_result).to receive(:fields) { %w[id some_field some_other_field another_field] }

        # Checksum queries
        expect(mysql_client).to receive(:prepare) { checksum_statement }
        expect(checksum_statement).to receive(:execute) { checksum_result }
        expect(checksum_result).to receive(:map) { build_list(:row_checksum, 1, row_id: "12") }

        checksums = subject.row_checksum("addresses", row_id: "12")
        checksum = checksums.first
        expect(checksum.row_id).to eq("12")
        expect(checksum.crc32).to_not be_nil
      end
    end
  end

  describe "#chunk_checksum" do
    let(:checksum_result) { double(Mysql2::Result) }
    let(:col_result) { double(Mysql2::Result) }
    let(:max_result) { double(Mysql2::Result) }
    let(:checksum_statement) { double(Mysql2::Statement) }
    let(:pk_statement) { double(Mysql2::Statement) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }

    context "when primary key cache is not empty" do
      context "when :limit is positive" do
        it "queries chunk_checksum with a set rowcount" do
          subject.primary_key_cache["addresses"] = "id"

          # Columns, Min, and Max queries
          allow(mysql_client).to receive(:query) do |*args|
            query = args.first
            if query.match?("as max")
              max_result
            elsif query.match?("as min")
              nil
            else
              col_result
            end
          end
          allow(max_result).to receive(:map) { [{"max" => "12345667"}] }
          allow(col_result).to receive(:fields) { %w[id some_field some_other_field another_field] }

          # Checksum queries
          expect(mysql_client).to receive(:prepare) { checksum_statement }
          expect(checksum_statement).to receive(:execute) { checksum_result }
          expect(checksum_result).to receive(:map) { build_list(:chunk_checksum, 1, min: "12") }

          checksum = subject.chunk_checksum("addresses", min: "12", limit: 128).first

          expect(checksum.min).to eq("12")
          expect(checksum.crc32).to_not be_nil
        end
      end
      context "when :limit is nil" do
        it "queries chunk_checksum with a set min and max rows" do
          subject.primary_key_cache["addresses"] = "id"

          # Columns, Min, and Max queries
          allow(mysql_client).to receive(:query) do |*args|
            query = args.first
            if query.match?("as max")
              max_result
            elsif query.match?("as min")
              nil
            else
              col_result
            end
          end
          allow(max_result).to receive(:map) { [{"max" => "12345667"}] }
          allow(col_result).to receive(:fields) { %w[id some_field some_other_field another_field] }

          # Checksum queries
          expect(mysql_client).to receive(:prepare) { checksum_statement }
          expect(checksum_statement).to receive(:execute) { checksum_result }
          expect(checksum_result).to receive(:map) { build_list(:chunk_checksum, 1, min: "12") }

          checksum = subject.chunk_checksum("addresses", min: "12", max: 23).first

          expect(checksum.min).to eq("12")
          expect(checksum.crc32).to_not be_nil
        end
      end
    end
  end
  describe "#generate_delete" do
    let(:pk_result) { double(Mysql2::Result) }
    let(:pk_statement) { double(Mysql2::Statement) }

    let(:row_checksum) { build(:row_checksum) }
    let(:select_all_statement) { double(Mysql2::Statement) }
    let(:select_all_result) { double(Mysql2::Result) }

    context "when database_name is set" do
      subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }
      it "queries existing rows and returns commands including database name" do
        expect(mysql_client).to receive(:prepare).twice do |*args|
          query = args.shift
          if query.match?("PRIMARY KEY")
            pk_statement
          elsif query.match?('select \\* from .+ where .+')
            select_all_statement
          else
            raise "missed all branches #{query}"
          end
        end

        expect(pk_statement).to receive(:execute) { pk_result }
        expect(pk_result).to receive(:map) { ["id"] }

        expect(select_all_statement).to receive(:execute).with(row_checksum.row_id) { select_all_result }
        expect(select_all_result).to receive(:each).and_yield({"id" => 12, "some_field" => "yada yada"})

        cmd = subject.generate_delete(row_checksum.table_name, row_checksum.row_id)
        expect(cmd).to include(a_string_matching(/production_database_name\.addresses/))
      end
    end
    context "when database_name is not set" do
      subject { MysqlConnection.new(client: mysql_client, logger: logger) }
      it "queries existing rows and returns commands not including database name" do
        expect(mysql_client).to receive(:prepare).twice do |*args|
          query = args.shift
          if query.match?("PRIMARY KEY")
            pk_statement
          elsif query.match?('select \\* from .+ where .+')
            select_all_statement
          else
            raise "missed all branches #{query}"
          end
        end

        expect(pk_statement).to receive(:execute) { pk_result }
        expect(pk_result).to receive(:map) { ["id"] }

        expect(select_all_statement).to receive(:execute) { select_all_result }
        expect(select_all_result).to receive(:each).and_yield({"id" => 12, "some_field" => "yada yada"})

        cmd = subject.generate_delete(row_checksum.table_name, row_checksum.row_id)
        expect(cmd).to_not include(a_string_matching(/production_database_name\.addresses/))
      end
    end
  end
  describe "#generate_insert" do
    let(:pk_result) { double(Mysql2::Result) }
    let(:pk_statement) { double(Mysql2::Statement) }

    let(:row_checksum) { build(:row_checksum) }
    let(:select_all_statement) { double(Mysql2::Statement) }
    let(:select_all_result) { double(Mysql2::Result) }

    context "when database_name is provided" do
      subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }
      it "queries existing rows" do
        expect(mysql_client).to receive(:prepare).twice do |*args|
          query = args.shift
          if query.match?("PRIMARY KEY")
            pk_statement
          elsif query.match?('select \\* from .+ where .+')
            select_all_statement
          else
            raise "missed all branches #{query}"
          end
        end

        expect(pk_statement).to receive(:execute) { pk_result }
        expect(pk_result).to receive(:map) { ["id"] }

        expect(select_all_statement).to receive(:execute) { select_all_result }
        expect(select_all_result).to receive(:each).and_yield({"id" => 12, "some_field" => "yada yada"})

        cmd = subject.generate_insert(row_checksum.table_name, row_checksum.row_id)
        expect(cmd).to include(a_string_matching(/production_database_name\.addresses/))
      end
    end
    context "when database_name is not provided" do
      subject { MysqlConnection.new(client: mysql_client, logger: logger) }
      it "queries existing rows" do
        expect(mysql_client).to receive(:prepare).twice do |*args|
          query = args.shift
          if query.match?("PRIMARY KEY")
            pk_statement
          elsif query.match?('select \\* from .+ where .+')
            select_all_statement
          else
            raise "missed all branches #{query}"
          end
        end

        expect(pk_statement).to receive(:execute) { pk_result }
        expect(pk_result).to receive(:map) { ["id"] }

        expect(select_all_statement).to receive(:execute) { select_all_result }
        expect(select_all_result).to receive(:each).and_yield({"id" => 12, "some_field" => "yada yada"})

        cmd = subject.generate_insert(row_checksum.table_name, row_checksum.row_id)
        expect(cmd).to_not include(a_string_matching(/production_database_name\.addresses/))
      end
    end
  end
  describe "#generate_update" do
    let(:pk_result) { double(Mysql2::Result) }
    let(:pk_statement) { double(Mysql2::Statement) }

    let(:row_checksum) { build(:row_checksum) }
    let(:select_all_statement) { double(Mysql2::Statement) }
    let(:select_all_result) { double(Mysql2::Result) }

    subject { MysqlConnection.new(client: mysql_client, database_name: database_name, logger: logger) }
    it "queries existing rows" do
      expect(mysql_client).to receive(:prepare).twice do |*args|
        query = args.shift
        if query.match?("PRIMARY KEY")
          pk_statement
        elsif query.match?('select \\* from .+ where .+')
          select_all_statement
        else
          raise "missed all branches #{query}"
        end
      end

      expect(pk_statement).to receive(:execute) { pk_result }
      expect(pk_result).to receive(:map) { ["id"] }

      expect(select_all_statement).to receive(:execute) { select_all_result }
      expect(select_all_result).to receive(:map) { [{"id" => 12, "some_field" => "yada yada"}] }

      subject.generate_update(row_checksum.table_name, row_checksum.row_id)
    end
  end
end
