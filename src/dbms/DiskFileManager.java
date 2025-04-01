package dbms;

import dbms.util.Constants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class DiskFileManager {
    private MetadataManager metadataManager;
    private int nextAvailablePosition; // 새로운 레코드를 저장할 다음 위치 추적

    public DiskFileManager(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        this.nextAvailablePosition = Constants.BLOCK_SIZE; // 첫 번째 블록 이후부터 시작

        // 데이터 디렉터리 생성
        File directory = new File(Constants.DATA_DIRECTORY);
        if (!directory.exists()) {
            directory.mkdirs();
        }
    }

    /**
     * 순차 파일 생성
     */
    public void createSequentialFile(String fileName, List<String> fieldNames,
                                     List<String> fieldTypes, List<Integer> fieldLengths) throws IOException, SQLException {
        // 파일 경로
        String filePath = Constants.DATA_DIRECTORY + fileName;

        // 파일 생성 및 초기화
        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength(0); // 파일 초기화

            // 헤더 블록 생성 및 초기화
            Block headerBlock = new Block();
            headerBlock.initializeHeaderBlock();

            // 헤더 블록 쓰기
            file.write(headerBlock.getData());
        }

        // 위치 초기화
        nextAvailablePosition = Constants.BLOCK_SIZE;

        System.out.println("순차 파일 생성 완료: " + fileName);
    }

    /**
     * 블록 읽기
     */
    public Block readBlock(String fileName, int blockNumber) throws IOException {
        String filePath = Constants.DATA_DIRECTORY + fileName;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "r")) {
            // 파일 크기 확인
            if (file.length() < (blockNumber + 1) * Constants.BLOCK_SIZE) {
                throw new IOException("존재하지 않는 블록 번호: " + blockNumber);
            }

            // 블록 위치로 이동
            file.seek(blockNumber * Constants.BLOCK_SIZE);

            // 블록 데이터 읽기
            byte[] blockData = new byte[Constants.BLOCK_SIZE];
            file.read(blockData);

            return new Block(blockData);
        }
    }

    /**
     * 블록 쓰기
     */
    public void writeBlock(String fileName, int blockNumber, Block block) throws IOException {
        String filePath = Constants.DATA_DIRECTORY + fileName;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            // 필요한 경우 파일 크기 확장
            if (file.length() < (blockNumber + 1) * Constants.BLOCK_SIZE) {
                file.setLength((blockNumber + 1) * Constants.BLOCK_SIZE);
            }

            // 블록 위치로 이동
            file.seek(blockNumber * Constants.BLOCK_SIZE);

            // 블록 데이터 쓰기
            file.write(block.getData());
        }
    }

    /**
     * 첫 레코드 포인터 가져오기
     */
    public int getFirstRecordPointer(String fileName) throws IOException {
        Block headerBlock = readBlock(fileName, 0);
        return headerBlock.getFirstRecordPointer();
    }

    /**
     * 첫 레코드 포인터 설정
     */
    public void setFirstRecordPointer(String fileName, int pointer) throws IOException {
        Block headerBlock = readBlock(fileName, 0);
        headerBlock.setFirstRecordPointer(pointer);
        writeBlock(fileName, 0, headerBlock);
    }

    /**
     * 블록에서 레코드 읽기
     */
    public Record readRecord(String fileName, int pointer) throws IOException, SQLException {
        if (pointer < 0) {
            return null; // 포인터가 없음
        }

        // 블록 번호와 블록 내 오프셋 계산
        int blockNumber = pointer / Constants.BLOCK_SIZE;
        int blockOffset = pointer % Constants.BLOCK_SIZE;

        // 블록 읽기
        Block block = readBlock(fileName, blockNumber);

        // 메타데이터 가져오기
        List<String> fieldNames = metadataManager.getFieldNames(fileName);
        List<Integer> fieldLengths = metadataManager.getFieldLengths(fileName);

        // 레코드 읽기를 위한 바이트 배열 준비
        byte[] blockData = block.getData();

        // 레코드 크기 추정 (최대 크기로 가정)
        int maxRecordSize = Constants.NULL_BITMAP_SIZE;
        for (int length : fieldLengths) {
            maxRecordSize += length;
        }
        maxRecordSize += Constants.POINTER_SIZE;

        // 실제 사용할 데이터 크기 (블록 경계를 넘어가지 않도록)
        int actualSize = Math.min(maxRecordSize, Constants.BLOCK_SIZE - blockOffset);

        // 블록 데이터에서 레코드 부분 추출
        byte[] recordData = new byte[actualSize];
        System.arraycopy(blockData, blockOffset, recordData, 0, actualSize);

        // 레코드 생성
        Record record = Record.fromBytes(recordData, fieldLengths, fieldNames);

        return record;
    }

    /**
     * 레코드 쓰기 (새 레코드 추가)
     * nextAvailablePosition을 사용하여 새로운 레코드 위치 할당
     */
    public int writeRecord(String fileName, Record record, int position) throws IOException {
        byte[] recordBytes = record.toBytes();
        int recordSize = recordBytes.length;

        // 다음 사용 가능한 위치 사용 (파일 초기화 시에는 항상 Constants.BLOCK_SIZE부터 시작)
        int newPosition = nextAvailablePosition;

        // 블록 번호와 블록 내 오프셋 계산
        int blockNumber = newPosition / Constants.BLOCK_SIZE;
        int blockOffset = newPosition % Constants.BLOCK_SIZE;

        // 블록이 꽉 찼는지 확인, 꽉 찼다면 다음 블록으로 이동
        if (blockOffset + recordSize > Constants.BLOCK_SIZE) {
            blockNumber++;
            blockOffset = 0;
            newPosition = blockNumber * Constants.BLOCK_SIZE;
        }

        // 블록 읽기
        Block block;
        try {
            block = readBlock(fileName, blockNumber);
        } catch (IOException e) {
            // 블록이 존재하지 않으면 새 블록 생성
            block = new Block();
        }

        // 블록에 레코드 쓰기
        System.arraycopy(recordBytes, 0, block.getData(), blockOffset, recordSize);

        // 블록 쓰기
        writeBlock(fileName, blockNumber, block);

        // 다음 사용 가능한 위치 업데이트
        nextAvailablePosition = newPosition + recordSize;

        // 레코드의 새 위치 반환
        return newPosition;
    }

    /**
     * 기존 레코드 업데이트
     */
    public void updateRecord(String fileName, Record record, int position) throws IOException {
        byte[] recordBytes = record.toBytes();
        int recordSize = recordBytes.length;

        // 블록 번호와 블록 내 오프셋 계산
        int blockNumber = position / Constants.BLOCK_SIZE;
        int blockOffset = position % Constants.BLOCK_SIZE;

        // 블록 읽기
        Block block = readBlock(fileName, blockNumber);

        // 블록에 레코드 쓰기
        System.arraycopy(recordBytes, 0, block.getData(), blockOffset, recordSize);

        // 블록 쓰기
        writeBlock(fileName, blockNumber, block);
    }

    /**
     * 파일 크기 (블록 수) 가져오기
     */
    public int getFileSize(String fileName) throws IOException {
        String filePath = Constants.DATA_DIRECTORY + fileName;

        File file = new File(filePath);
        if (!file.exists()) {
            return 0;
        }

        return (int) (file.length() / Constants.BLOCK_SIZE);
    }

    /**
     * 파일이 디스크에 존재하는지 확인
     */
    public boolean fileExistsOnDisk(String fileName) {
        String filePath = Constants.DATA_DIRECTORY + fileName;
        File file = new File(filePath);
        return file.exists();
    }

    /**
     * 다음 가용 위치 재설정
     */
    public void resetNextAvailablePosition() {
        nextAvailablePosition = Constants.BLOCK_SIZE;
    }

    /**
     * 현재 다음 사용 가능한 위치 가져오기
     */
    public int getNextAvailablePosition() {
        return nextAvailablePosition;
    }

    /**
     * 파일 통계 정보 가져오기
     */
    public FileStats getFileStats(String fileName) throws IOException {
        String filePath = Constants.DATA_DIRECTORY + fileName;
        File file = new File(filePath);

        if (!file.exists()) {
            throw new IOException("파일이 존재하지 않습니다: " + fileName);
        }

        // 전체 파일 크기 (바이트)
        long fileSize = file.length();

        // 블록 개수
        int blockCount = (int) (fileSize / Constants.BLOCK_SIZE);

        // 첫 레코드 포인터
        int firstRecordPointer = getFirstRecordPointer(fileName);

        // 레코드 개수와 평균 크기 계산
        int recordCount = 0;
        int totalRecordSize = 0;
        int currentPointer = firstRecordPointer;

        // 레코드 포인터 추적 (순환 감지용)
        boolean[] visitedPointers = new boolean[10000];

        while (currentPointer >= 0) {
            // 순환 감지
            if (visitedPointers[currentPointer]) {
                break;
            }
            visitedPointers[currentPointer] = true;

            try {
                // 레코드 읽기
                Record record = readRecord(fileName, currentPointer);
                if (record == null) {
                    break;
                }

                recordCount++;

                // 다음 레코드 위치를 기준으로 현재 레코드 크기 추정
                int nextPointer = record.getNextPointer();
                if (nextPointer > currentPointer) {
                    totalRecordSize += (nextPointer - currentPointer);
                } else {
                    // 마지막 레코드거나 다음 포인터가 이전 블록을 가리키는 경우
                    // 레코드 직접 계산
                    totalRecordSize += record.calculateRecordSize();
                }

                // 다음 레코드로 이동
                currentPointer = nextPointer;
            } catch (Exception e) {
                break;
            }
        }

        // 평균 레코드 크기
        double avgRecordSize = recordCount > 0 ? (double) totalRecordSize / recordCount : 0;

        // blocking factor
        double blockingFactor = avgRecordSize > 0 ? Constants.BLOCK_SIZE / avgRecordSize : 0;

        return new FileStats(fileSize, blockCount, recordCount, avgRecordSize, blockingFactor);
    }

    /**
     * 파일 통계 정보를 담는 내부 클래스
     */
    public static class FileStats {
        private long fileSize;
        private int blockCount;
        private int recordCount;
        private double avgRecordSize;
        private double blockingFactor;

        public FileStats(long fileSize, int blockCount, int recordCount, double avgRecordSize, double blockingFactor) {
            this.fileSize = fileSize;
            this.blockCount = blockCount;
            this.recordCount = recordCount;
            this.avgRecordSize = avgRecordSize;
            this.blockingFactor = blockingFactor;
        }

        @Override
        public String toString() {
            return String.format(
                    "파일 크기: %d 바이트%n" +
                            "블록 개수: %d%n" +
                            "레코드 개수: %d%n" +
                            "평균 레코드 크기: %.2f 바이트%n" +
                            "Blocking Factor: %.2f",
                    fileSize, blockCount, recordCount, avgRecordSize, blockingFactor
            );
        }
    }
}