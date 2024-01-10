require 'rspec'

require_relative '../mysql_connection'
require_relative '../table'

describe 'ChunkComparison' do
  describe '#new' do
    let(:master_checksum) { build(:chunk_checksum) }
    let(:replica_checksum) { build(:chunk_checksum) }
    subject do
      ChunkComparison.new(master: master_checksum, replica: replica_checksum)
    end

    it 'assigns master correctly' do
      expect(subject.master).to be(master_checksum)
    end
    it 'assigns replica correctly' do
      expect(subject.replica).to be(replica_checksum)
    end
  end
  describe '#compare' do
    context 'when checksum objects are equal' do
      let(:master_checksum) { build(:chunk_checksum, min: '12', max: '128', count: '99', crc32: 111_111) }
      let(:replica_checksum) { build(:chunk_checksum, min: '12', max: '128', count: '99', crc32: 111_111) }
      subject do
        ChunkComparison.new(master: master_checksum, replica: replica_checksum)
      end
      it 'returns true' do
        expect(subject.compare).to be_truthy
      end
    end
  end
  context 'when checksum objects are unequal' do
    # We'll trust FactoryBot to create two different checksum objects
    let(:master_checksum) { build(:chunk_checksum) }
    let(:replica_checksum) { build(:chunk_checksum) }
    subject do
      ChunkComparison.new(master: master_checksum, replica: replica_checksum)
    end
    it 'returns false' do
      expect(subject.compare).to be_falsey
    end
  end
end

describe 'RowComparison' do
  describe '#new' do
    let(:master_checksum) { build(:row_checksum) }
    let(:replica_checksum) { build(:row_checksum) }
    subject do
      RowComparison.new(master: master_checksum, replica: replica_checksum)
    end

    it 'assigns master correctly' do
      expect(subject.master).to be(master_checksum)
    end
    it 'assigns replica correctly' do
      expect(subject.replica).to be(replica_checksum)
    end
  end
  describe '#from' do
    context 'when given master' do
      let(:master_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(master: master_checksum)
      end
      it 'populates the master field' do
        expect(subject.master).to eq(master_checksum)
      end
      it 'has a null replica' do
        expect(subject.replica).to be_nil
      end
      it 'populates the table_name field' do
        expect(subject.table_name).to eq(master_checksum.table_name)
      end
      it 'populates the row_id fiel' do
        expect(subject.row_id).to be(master_checksum.row_id)
      end
    end
    context 'when given replica' do
      let(:replica_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(replica: replica_checksum)
      end
      it 'has a null master' do
        expect(subject.master).to be_nil
      end
      it 'populates the replica field' do
        expect(subject.replica).to eq(replica_checksum)
      end
      it 'populates the table_name field' do
        expect(subject.table_name).to eq(replica_checksum.table_name)
      end
      it 'populates the row_id fiel' do
        expect(subject.row_id).to be(replica_checksum.row_id)
      end
    end
  end
  describe '#master?' do
    context 'when master is nil' do
      let(:replica_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(replica: replica_checksum)
      end
      it 'returns falsey' do
        expect(subject.master?).to be_falsey
      end
    end
    context 'when master is not nil' do
      let(:master_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(master: master_checksum)
      end
      it 'returns truthy' do
        expect(subject.master?).to be_truthy
      end
    end
  end
  describe '#replica?' do
    context 'when replica is nil' do
      let(:master_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(master: master_checksum)
      end
      it 'returns falsey' do
        expect(subject.replica?).to be_falsey
      end
    end
    context 'when replica is not nil' do
      let(:replica_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(replica: replica_checksum)
      end
      it 'returns truthy' do
        expect(subject.replica?).to be_truthy
      end
    end
  end
  describe '#primary_key' do
    context 'when master?' do
      let(:master_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(master: master_checksum)
      end
      it 'returns the master primary_key' do
        expect(subject.primary_key).to eq(master_checksum.primary_key)
      end
    end
    context 'when replica?' do
      let(:replica_checksum) { build(:row_checksum) }
      subject do
        RowComparison.from(replica: replica_checksum)
      end
      it 'returns the replica primary_key' do
        expect(subject.primary_key).to eq(replica_checksum.primary_key)
      end
    end
  end
  describe '#compare' do
    context 'when checksum objects are equal' do
      let(:master_checksum) { build(:row_checksum, row_id: '128', crc32: 111_111) }
      let(:replica_checksum) { build(:row_checksum, row_id: '128', crc32: 111_111) }
      subject do
        RowComparison.new(master: master_checksum, replica: replica_checksum)
      end
      it 'returns true' do
        expect(subject.compare).to be_truthy
      end
    end
  end
  context 'when checksum objects are unequal' do
    let(:master_checksum) { build(:row_checksum) }
    let(:replica_checksum) { build(:row_checksum) }
    subject do
      RowComparison.new(master: master_checksum, replica: replica_checksum)
    end
    it 'returns false' do
      expect(subject.compare).to be_falsey
    end
  end
end
