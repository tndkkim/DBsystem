package dbms;

import dbms.util.ByteUtils;
import dbms.util.Constants;
import dbms.util.RecordPointer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.*;

public class DiskFileManager {
    private MetadataManager metadataManager;
    private Map<String, Integer> nextAvailablePositions = new HashMap<>();

    private Map<String, Map<Integer, byte[]>> blockCache = new HashMap<>();
    private static final int CACHE_SIZE = 3;

    public DiskFileManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;

        // 데이터 디렉터리 생성
        File directory = new File(Constants.DATA_DIRECTORY);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    public void createSequentialFile(String fileName, List<String> fieldNames,
                                     List<String> fieldTypes, List<Integer> fieldLengths) throws IOException, SQLException {
        String filePath = Constants.DATA_DIRECTORY + fileName;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength(0);
            byte[] headerBlockData = new byte[Constants.BLOCK_SIZE];

            for (int i = 0; i < Constants.POINTER_SIZE; i++) {
                headerBlockData[i] = (byte) 0xFF;
            }

            file.write(headerBlockData);
        }

        nextAvailablePositions.put(fileName, Constants.BLOCK_SIZE);
        blockCache.put(fileName, new LinkedHashMap<Integer, byte[]>(CACHE_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                return size() > CACHE_SIZE;
            }
        });

        System.out.println("순차 파일 생성 완료: " + fileName);
    }

    public byte[] readBlockData(String fileName, int blockNumber) throws IOException {
        Map<Integer, byte[]> fileCache = blockCache.get(fileName);
        if (fileCache != null && fileCache.containsKey(blockNumber)) {
            return Arrays.copyOf(fileCache.get(blockNumber), Constants.BLOCK_SIZE);
        }

        String filePath = Constants.DATA_DIRECTORY + fileName;
        long position = (long) blockNumber * Constants.BLOCK_SIZE;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {

            file.seek(position);

            byte[] blockData = new byte[Constants.BLOCK_SIZE];
            file.readFully(blockData);

            if (fileCache == null) {
                fileCache = new LinkedHashMap<>(CACHE_SIZE, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                        return size() > CACHE_SIZE;
                    }
                };
                blockCache.put(fileName, fileCache);
            }
            fileCache.put(blockNumber, blockData);

            return blockData;
        }
    }

    public void writeBlockData(String fileName, int blockNumber, byte[] blockData) throws IOException {

        String filePath = Constants.DATA_DIRECTORY + fileName;
        long position = (long) blockNumber * Constants.BLOCK_SIZE;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            if (file.length() < position + Constants.BLOCK_SIZE) {
                file.setLength(position + Constants.BLOCK_SIZE);
            }

            file.seek(position);

            file.write(blockData);

            Map<Integer, byte[]> fileCache = blockCache.get(fileName);
            if (fileCache == null) {
                fileCache = new LinkedHashMap<>(CACHE_SIZE, 0.75f, true) {
                    @Override
                    protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                        return size() > CACHE_SIZE;
                    }
                };
                blockCache.put(fileName, fileCache);
            }
            fileCache.put(blockNumber, Arrays.copyOf(blockData, Constants.BLOCK_SIZE));
        }
    }


    // 첫 레코드 포인터 값
    public int getFirstRecordPointer(String fileName) throws IOException {
        byte[] headerBlock = readBlockData(fileName, 0);

        boolean isNegativeOne = true;
        for (int i = 0; i < Constants.POINTER_SIZE; i++) {
            if (headerBlock[i] != (byte) 0xFF) {
                isNegativeOne = false;
                break;
            }
        }

        if (isNegativeOne) {
            return -1;
        }

        int blockNumber = ((headerBlock[0] & 0xFF) << 24) |
                ((headerBlock[1] & 0xFF) << 16) |
                ((headerBlock[2] & 0xFF) << 8) |
                (headerBlock[3] & 0xFF);

        int offsetInBlock = ((headerBlock[4] & 0xFF) << 8) |
                (headerBlock[5] & 0xFF);

        //System.out.println("블록 번호 : " + blockNumber + "오프셋: "+ offsetInBlock);

        return blockNumber * Constants.BLOCK_SIZE + offsetInBlock;
        }

    public int pointerToOffset(RecordPointer pointer) {
        if (pointer == null) {
            return -1;
        }
        return pointer.toFileOffset(Constants.BLOCK_SIZE);
    }

    public RecordPointer offsetToPointer(int offset) {
        if (offset < 0) {
            return null;
        }

        int blockNumber = offset / Constants.BLOCK_SIZE;
        int offsetInBlock = offset % Constants.BLOCK_SIZE;

        return new RecordPointer(blockNumber, offsetInBlock);
    }

    public Record readRecord(String fileName, RecordPointer pointer) throws IOException, SQLException {
        if (pointer == null) {
            return null;
        }

        int blockNumber = pointer.getBlockNumber();
        int offsetInBlock = pointer.getOffsetInBlock();

        List<String> fieldNames = metadataManager.getFieldNames(fileName);
        List<Integer> fieldLengths = metadataManager.getFieldLengths(fileName);

        try {
            byte[] blockData = readBlockData(fileName, blockNumber);

            byte nullBitmap = blockData[offsetInBlock];

            int recordSize = 1;
            for (int i = 0; i < fieldNames.size(); i++) {
                boolean isNull = ByteUtils.isFieldNull(nullBitmap, i);
                if (!isNull) {
                    recordSize += fieldLengths.get(i);
                }
            }
            recordSize += Constants.POINTER_SIZE;

            byte[] recordData = new byte[recordSize];

            int availableInBlock = Constants.BLOCK_SIZE - offsetInBlock;

            if (availableInBlock >= recordSize) {
                System.arraycopy(blockData, offsetInBlock, recordData, 0, recordSize);
            } else {
                System.arraycopy(blockData, offsetInBlock, recordData, 0, availableInBlock);

                int bytesRead = availableInBlock;
                int remainingBytes = recordSize - availableInBlock;
                int nextBlockNumber = blockNumber + 1;

                while (remainingBytes > 0) {
                    byte[] nextBlockData = readBlockData(fileName, nextBlockNumber);

                    int bytesToRead = Math.min(remainingBytes, Constants.BLOCK_SIZE);

                    System.arraycopy(nextBlockData, 0, recordData, bytesRead, bytesToRead);

                    bytesRead += bytesToRead;
                    remainingBytes -= bytesToRead;
                    nextBlockNumber++;
                }
            }

            return Record.fromBytes(recordData, fieldLengths, fieldNames);

        } catch (IOException e) {
            System.err.println("레코드 읽기 오류: " + e.getMessage());
            throw e;
        }
    }

    public int getFileSize(String fileName) throws IOException {
        String filePath = Constants.DATA_DIRECTORY + fileName;

        File file = new File(filePath);
        if (!file.exists()) {
            return 0;
        }

        return (int) Math.ceil(file.length() / (double) Constants.BLOCK_SIZE);
    }

    public void updateNextAvailablePosition(String fileName, int newPosition) {
        nextAvailablePositions.put(fileName, newPosition);
    }

    public int getNextAvailablePosition(String fileName) {
        return nextAvailablePositions.getOrDefault(fileName, Constants.BLOCK_SIZE);
    }

    public void resetFile(String fileName) throws IOException {
        byte[] headerBlock = new byte[Constants.BLOCK_SIZE];
        Arrays.fill(headerBlock, 0, Constants.POINTER_SIZE, (byte) 0xFF);

        writeBlockData(fileName, 0, headerBlock);

        updateNextAvailablePosition(fileName, Constants.BLOCK_SIZE);
        blockCache.put(fileName, new LinkedHashMap<>(CACHE_SIZE, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Integer, byte[]> eldest) {
                return size() > CACHE_SIZE;
            }
        });
    }
}