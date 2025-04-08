package dbms.util;

public class RecordPointer {
    private final int blockNumber;
    private final int offsetInBlock;

    public RecordPointer(int blockNumber, int offsetInBlock) {
        this.blockNumber = blockNumber;
        this.offsetInBlock = offsetInBlock;
    }

    public int toFileOffset(int blockSize) {
        return blockNumber * blockSize + offsetInBlock;
    }

    public int getBlockNumber() {
        return blockNumber;
    }

    public int getOffsetInBlock() {
        return offsetInBlock;
    }

    @Override
    public String toString() {
        return "Block #" + blockNumber + ", Offset: " + offsetInBlock;
    }
}
