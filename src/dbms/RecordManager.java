package dbms;

import dbms.util.Constants;
import dbms.util.RecordPointer;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public class RecordManager {
    private DiskFileManager diskFileManager;
    private MetadataManager metadataManager;

    public RecordManager(DiskFileManager diskFileManager, MetadataManager metadataManager) {
        this.diskFileManager = diskFileManager;
        this.metadataManager = metadataManager;
    }

    public void bulkInsertRecords(String fileName, String dataFilePath) throws IOException, SQLException {

        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        List<String> fieldNames = metadataManager.getFieldNames(fileName);
        List<Integer> fieldLengths = metadataManager.getFieldLengths(fileName);

        resetFile(fileName);

        try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {
            String fileNameLine = reader.readLine();
            String recordCountLine = reader.readLine();

            byte[] headerBlock = new byte[Constants.BLOCK_SIZE];
            Arrays.fill(headerBlock, 0, Constants.POINTER_SIZE, (byte) 0xFF); // 첫 레코드 포인터 -1로 설정
            diskFileManager.writeBlockData(fileName, 0, headerBlock);

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                String[] values = line.split(Constants.DELIMITER);
                List<String> fieldValues = new ArrayList<>();

                for (int i = 0; i < values.length; i++) {
                    String value = values[i].trim();
                    if (Constants.NULL_VALUE.equalsIgnoreCase(value)) {
                        fieldValues.add(null);
                    } else {
                        fieldValues.add(value);
                    }
                }

                while (fieldValues.size() < fieldNames.size()) {
                    fieldValues.add(null);
                }

                Record record = new Record(fieldValues, fieldLengths, fieldNames);

                insertRecordSorted(fileName, record);
            }

        } catch (IOException e) {
            System.err.println("파일 읽기 오류: " + e.getMessage());
            throw e;
        }
    }

    private void insertRecordSorted(String fileName, Record newRecord) throws IOException, SQLException {

        int firstRecordOffset = diskFileManager.getFirstRecordPointer(fileName);
        RecordPointer firstRecordPointer = diskFileManager.offsetToPointer(firstRecordOffset);

//        System.out.println("첫 레코드 포인터: " +
//                (firstRecordPointer != null ?
//                        "블록=" + firstRecordPointer.getBlockNumber() +
//                                ", 오프셋=" + firstRecordPointer.getOffsetInBlock() : "없음(-1)"));

        String newKey = newRecord.getSearchKey();

        RecordPointer currentPointer = firstRecordPointer;
        Record currentRecord = null;

        if (currentPointer != null) {
            boolean insertAtBeginning = false;

            Record firstRecord = diskFileManager.readRecord(fileName, currentPointer);

            if (firstRecord == null) {
                insertAtBeginning = true;
            }
            else if (newKey.compareTo(firstRecord.getSearchKey()) < 0) {
                insertAtBeginning = true;
            } else {
                currentRecord = firstRecord;

                while (currentRecord != null) {
                    int nextOffset = currentRecord.getNextPointer();

                    if (nextOffset < 0) {
                        break;
                    }

                    RecordPointer nextPointer = diskFileManager.offsetToPointer(nextOffset);
                    if (nextPointer == null) {
                        break;
                    }

                    Record nextRecord = diskFileManager.readRecord(fileName, nextPointer);
                    if (nextRecord == null) {
                        break;
                    }

                    if (nextRecord.getSearchKey().compareTo(newKey) > 0) {
                        break;
                    }

                    currentPointer = nextPointer;
                    currentRecord = nextRecord;
                }
            }
            RecordPointer newRecordPointer = calculateNewRecordPosition(fileName, newRecord);

            if (insertAtBeginning) {
                newRecord.setNextPointer(diskFileManager.pointerToOffset(firstRecordPointer));

                byte[] headerBlock = diskFileManager.readBlockData(fileName, 0);
                updatePointerInBlock(headerBlock, 0, newRecordPointer);
                diskFileManager.writeBlockData(fileName, 0, headerBlock);
            } else {
                newRecord.setNextPointer(currentRecord.getNextPointer());
                currentRecord.setNextPointer(diskFileManager.pointerToOffset(newRecordPointer));

                saveRecord(fileName, currentRecord, currentPointer);
            }

            saveRecord(fileName, newRecord, newRecordPointer);

        } else {
            RecordPointer newRecordPointer = calculateNewRecordPosition(fileName, newRecord);
            newRecord.setNextPointer(-1);

            byte[] headerBlock = diskFileManager.readBlockData(fileName, 0);
            updatePointerInBlock(headerBlock, 0, newRecordPointer);
            diskFileManager.writeBlockData(fileName, 0, headerBlock);

            saveRecord(fileName, newRecord, newRecordPointer);
        }

        //printRecordChain(fileName); //디버깅 코드
    }
    private void updatePointerInBlock(byte[] blockData, int offset, RecordPointer pointer) {
        int blockNumber = pointer.getBlockNumber();
        int offsetInBlock = pointer.getOffsetInBlock();

        // 블록 번호 (4바이트)
        blockData[offset] = (byte)((blockNumber >> 24) & 0xFF);
        blockData[offset + 1] = (byte)((blockNumber >> 16) & 0xFF);
        blockData[offset + 2] = (byte)((blockNumber >> 8) & 0xFF);
        blockData[offset + 3] = (byte)(blockNumber & 0xFF);

        // 오프셋 (2바이트)
        blockData[offset + 4] = (byte)((offsetInBlock >> 8) & 0xFF);
        blockData[offset + 5] = (byte)(offsetInBlock & 0xFF);
    }

    private RecordPointer calculateNewRecordPosition(String fileName, Record record) throws IOException {
        int nextPosition = diskFileManager.getNextAvailablePosition(fileName);
        int blockNumber = nextPosition / Constants.BLOCK_SIZE;
        int blockOffset = nextPosition % Constants.BLOCK_SIZE;

        // 현재 레코드 실제 크기 계산
        int recordSize = record.calculateRecordSize();

        // 블록 내 남은 공간 계산
        int freeSpace = Constants.BLOCK_SIZE - blockOffset;

        if (freeSpace < recordSize) {
            blockNumber++;
            blockOffset = 0;
        }

        return new RecordPointer(blockNumber, blockOffset);
    }
    private void saveRecord(String fileName, Record record, RecordPointer pointer) throws IOException {
        int blockNumber = pointer.getBlockNumber();
        int blockOffset = pointer.getOffsetInBlock();

        byte[] recordBytes = record.toBytes();
        int recordSize = recordBytes.length;

        int pointerOffset = recordBytes.length - Constants.POINTER_SIZE;
        StringBuilder nextPointerBytes = new StringBuilder("[");
        for (int i = 0; i < Constants.POINTER_SIZE; i++) {
            nextPointerBytes.append(String.format("%02X", recordBytes[pointerOffset + i] & 0xFF));
            if (i < Constants.POINTER_SIZE - 1) {
                nextPointerBytes.append(" ");
            }
        }
        nextPointerBytes.append("]");
        byte[] blockData;
        try {
            blockData = diskFileManager.readBlockData(fileName, blockNumber);
        } catch (IOException e) {
            blockData = new byte[Constants.BLOCK_SIZE];
        }

        if (blockOffset + recordSize <= Constants.BLOCK_SIZE) {
            System.arraycopy(recordBytes, 0, blockData, blockOffset, recordSize);

            diskFileManager.writeBlockData(fileName, blockNumber, blockData);
        } else {
            int firstPartSize = Constants.BLOCK_SIZE - blockOffset;

            System.arraycopy(recordBytes, 0, blockData, blockOffset, firstPartSize);
            diskFileManager.writeBlockData(fileName, blockNumber, blockData);

            byte[] nextBlockData = new byte[Constants.BLOCK_SIZE];
            System.arraycopy(recordBytes, firstPartSize, nextBlockData, 0, recordSize - firstPartSize);
            diskFileManager.writeBlockData(fileName, blockNumber + 1, nextBlockData);
        }

        // 다음 가용 위치 업데이트
        int newPosition = pointer.toFileOffset(Constants.BLOCK_SIZE) + recordSize;
        diskFileManager.updateNextAvailablePosition(fileName, newPosition);
    }
//    private void printRecordChain(String fileName) {
//        try {
//            int firstOffset = diskFileManager.getFirstRecordPointer(fileName);
//
//            RecordPointer pointer = diskFileManager.offsetToPointer(firstOffset);
//            int count = 0;
//
//            while (pointer != null && count < 100) { // 안전장치
//                count++;
//                Record record = diskFileManager.readRecord(fileName, pointer);
//
//                int nextOffset = record.getNextPointer();
//                System.out.println("레코드 #" + count + ": 키=" + record.getSearchKey() +
//                        ", 다음 포인터=" + nextOffset);
//
//                if (nextOffset < 0) {
//                    break;
//                }
//
//                pointer = diskFileManager.offsetToPointer(nextOffset);
//            }
//
//        } catch (Exception e) {
//            System.err.println("[DEBUG] 체인 출력 중 오류: " + e.getMessage());
//        }
//    }
    private void resetFile(String fileName) throws IOException {
        // 파일 초기화 (첫 레코드 포인터를 -1로 설정)
        diskFileManager.resetFile(fileName);
        System.out.println("파일 생성 및 초기화가 완료되었습니다.");
    }

    public List<String> searchField(String fileName, String fieldName) throws IOException, SQLException {
        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        int fieldIndex = metadataManager.getFieldIndex(fileName, fieldName);
        if (fieldIndex == -1) {
            System.err.println("존재하지 않는 필드: " + fieldName);
            throw new IllegalArgumentException("존재하지 않는 필드: " + fieldName);
        }

        List<String> results = new ArrayList<>();
        int firstOffset = diskFileManager.getFirstRecordPointer(fileName);

        long fileSize = 0;
        try {
            fileSize = diskFileManager.getFileSize(fileName) * Constants.BLOCK_SIZE;
        } catch (Exception e) {
            System.err.println("파일 크기 확인 중 오류: " + e.getMessage());
        }

        int recordCount = 0;
        int maxRecords = 1000;

        // 현재 처리 중인 레코드 오프셋
        int currentOffset = firstOffset;

        while (currentOffset >= 0 && recordCount < maxRecords) {
            recordCount++;

            if (currentOffset < 0 || currentOffset >= fileSize) {
                break;
            }

            // 블록 번호, 오프셋 계산
            int blockNumber = currentOffset / Constants.BLOCK_SIZE;
            int blockOffset = currentOffset % Constants.BLOCK_SIZE;
            RecordPointer currentPointer = new RecordPointer(blockNumber, blockOffset);

            Record record;
            try {
                record = diskFileManager.readRecord(fileName, currentPointer);
            } catch (Exception e) {
                System.err.println("레코드 읽기 실패: " + e.getMessage());
                break;
            }

            if (record == null) {
                System.err.println("레코드 읽기 실패: null 반환됨");
                break;
            }

            String fieldValue = record.getFieldValue(fieldName);
            results.add(fieldValue);

            int nextOffset = record.getNextPointer();

            if (nextOffset < -1) {
                break;
            }

            if (nextOffset > 0 && nextOffset >= fileSize) {
                break;
            }

            if (nextOffset == currentOffset && nextOffset != -1) {
                break;
            }

            currentOffset = nextOffset;
        }

        System.out.println("검색 결과 수: " + results.size());

        return results;
    }

    public List<Record> searchRecords(String fileName, String minKey, String maxKey) throws IOException, SQLException {
        System.out.println("검색 조건: " + minKey + " <= 키 <= " + maxKey);

        if (!metadataManager.fileExists(fileName)) {
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        List<Record> results = new ArrayList<>();

        int firstOffset = diskFileManager.getFirstRecordPointer(fileName);

        int currentOffset = firstOffset;
        while (currentOffset >= 0) {

            int blockNumber = currentOffset / Constants.BLOCK_SIZE;
            int blockOffset = currentOffset % Constants.BLOCK_SIZE;
            //System.out.println("블록 번호 : " + blockNumber + "오프셋 :" + blockOffset);
            RecordPointer currentPointer = new RecordPointer(blockNumber, blockOffset);

            Record record;
            try {
                record = diskFileManager.readRecord(fileName, currentPointer);
            } catch (Exception e) {
                break;
            }

            if (record == null) {
                break;
            }

            String key = record.getSearchKey();

            if (key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0) {
                results.add(record);
            }

            if (key.compareTo(maxKey) > 0) {
                break;
            }

            int nextOffset = record.getNextPointer();

            currentOffset = nextOffset;
        }

        System.out.println("검색 결과 수: " + results.size());

        return results;
    }

}