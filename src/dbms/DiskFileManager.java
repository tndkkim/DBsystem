package dbms;

import dbms.util.Constants;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.sql.SQLException;
import java.util.*;

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

    public void createSequentialFile(String fileName, List<String> fieldNames,
                                     List<String> fieldTypes, List<Integer> fieldLengths) throws IOException, SQLException {

        String filePath = Constants.DATA_DIRECTORY + fileName;

        try (RandomAccessFile file = new RandomAccessFile(filePath, "rw")) {
            file.setLength(0); // 파일 초기화

            // 헤더 블록 생성 및 초기화
            Block headerBlock = new Block();
            headerBlock.initializeHeaderBlock();

            // 헤더 블록 쓰기
            file.write(headerBlock.getData());
        }

        // 첫번째 레코드 저장 위치
        nextAvailablePosition = Constants.BLOCK_SIZE;

        System.out.println("순차 파일 생성 완료: " + fileName);
    }

    //블록 읽기
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

    // 첫 레코드 포인터 값
    public int getFirstRecordPointer(String fileName) throws IOException {
        Block headerBlock = readBlock(fileName, 0); //첫번째 블록 == 0
        return headerBlock.getFirstRecordPointer(); //
    }

    /**
     * 블록에서 레코드 읽기 - 블록 경계를 넘는 레코드도 처리할 수 있도록 수정
     * 포인터 유효성 검사 강화
     */
    public Record readRecord(String fileName, int pointer) throws IOException, SQLException {
        if (pointer < 0) {
            System.out.println("유효하지 않은 포인터 (음수): " + pointer);
            return null; // 포인터가 없음
        }

        String filePath = Constants.DATA_DIRECTORY + fileName;
        File file = new File(filePath);

        if (!file.exists()) {
            System.out.println("파일이 존재하지 않습니다: " + fileName);
            return null;
        }

        // 파일 크기 확인
        long fileSize = file.length();
        if (pointer >= fileSize) {
            System.out.println("잘못된 포인터 값: " + pointer + ", 파일 크기: " + fileSize);
            return null;
        }

        try (RandomAccessFile raf = new RandomAccessFile(filePath, "r")) {
            // 메타데이터 가져오기
            List<String> fieldNames = metadataManager.getFieldNames(fileName);
            List<Integer> fieldLengths = metadataManager.getFieldLengths(fileName);

            // 블록 번호와 블록 내 오프셋 계산
            int blockNumber = pointer / Constants.BLOCK_SIZE;
            int blockOffset = pointer % Constants.BLOCK_SIZE;

            // 먼저 null bitmap 읽기 (첫 바이트)
            raf.seek(pointer);
            byte nullBitmap = raf.readByte();

            // null이 아닌 필드 개수 계산
            int nonNullFieldCount = 0;
            for (int i = 0; i < fieldNames.size(); i++) {
                // 해당 비트가 1이면 필드는 null
                boolean isNull = ((nullBitmap >> i) & 1) == 1;
                if (!isNull) {
                    nonNullFieldCount++;
                }
            }

            // 레코드 총 크기 계산 (null bitmap + 실제 필드 데이터 + 포인터)
            int recordSize = Constants.NULL_BITMAP_SIZE;
            for (int i = 0; i < fieldNames.size(); i++) {
                // 해당 비트가 1이면 필드는 null, 0이면 필드 데이터 존재
                boolean isNull = ((nullBitmap >> i) & 1) == 1;
                if (!isNull) {
                    recordSize += fieldLengths.get(i);
                }
            }
            recordSize += Constants.POINTER_SIZE; // 다음 레코드 포인터

            // 레코드 크기 유효성 검사 (지나치게 큰 경우 오류 가능성)
            int maxExpectedRecordSize = Constants.NULL_BITMAP_SIZE +
                    fieldNames.size() * 100 + // 필드당 최대 예상 크기
                    Constants.POINTER_SIZE;

            if (recordSize <= 0 || recordSize > maxExpectedRecordSize) {
                System.out.println("계산된 레코드 크기가 비정상적입니다: " + recordSize);
                return null;
            }

            // 전체 레코드 데이터를 저장할 바이트 배열
            byte[] recordData = new byte[recordSize];

            // null bitmap은 이미 읽었으므로 다시 파일 위치를 포인터로 설정
            raf.seek(pointer);

            try {
                // 레코드 데이터 읽기 (블록 경계를 고려하여)
                int bytesRead = 0;
                int bytesToRead = recordSize;

                // 남은 블록 크기 계산
                int remainingInBlock = Constants.BLOCK_SIZE - blockOffset;

                if (remainingInBlock >= recordSize) {
                    // 레코드가 현재 블록 내에 모두 포함됨
                    raf.readFully(recordData, 0, recordSize);
                } else {
                    // 레코드가 블록 경계를 넘어감
                    System.out.println("레코드가 블록 경계를 넘어감: 포인터=" + pointer +
                            ", 레코드 크기=" + recordSize +
                            ", 현재 블록에 남은 공간=" + remainingInBlock);

                    // 현재 블록에서 읽을 수 있는 만큼 읽기
                    raf.readFully(recordData, 0, remainingInBlock);
                    bytesRead = remainingInBlock;
                    bytesToRead -= remainingInBlock;

                    // 다음 블록(들)에서 나머지 부분 읽기
                    while (bytesToRead > 0) {
                        // 다음 블록으로 이동
                        blockNumber++;
                        int nextBlockPosition = blockNumber * Constants.BLOCK_SIZE;

                        // 파일 크기 벗어나는지 확인
                        if (nextBlockPosition + bytesToRead > fileSize) {
                            System.out.println("레코드가 파일 크기를 초과합니다: " +
                                    (nextBlockPosition + bytesToRead) + " > " + fileSize);
                            return null;
                        }

                        raf.seek(nextBlockPosition);

                        // 한 블록에서 읽을 수 있는 최대 바이트 계산
                        int bytesToReadFromBlock = Math.min(bytesToRead, Constants.BLOCK_SIZE);

                        // 읽기
                        raf.readFully(recordData, bytesRead, bytesToReadFromBlock);

                        bytesRead += bytesToReadFromBlock;
                        bytesToRead -= bytesToReadFromBlock;
                    }
                }

                // 레코드 생성
                Record record = Record.fromBytes(recordData, fieldLengths, fieldNames);

                // 다음 포인터 유효성 검사
                int nextPointer = record.getNextPointer();
                if (nextPointer > 0 && nextPointer >= fileSize) {
                    System.out.println("경고: 다음 포인터가 파일 크기를 초과합니다: " + nextPointer + " >= " + fileSize);
                    record.setNextPointer(-1); // 안전하게 마지막 레코드로 설정
                }

                return record;
            } catch (Exception e) {
                System.out.println("레코드 읽기 중 예외 발생: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        } catch (Exception e) {
            System.out.println("파일 열기 중 예외 발생: " + e.getMessage());
            e.printStackTrace();
            return null;
        }
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

        return (int) Math.ceil(file.length() / (double)Constants.BLOCK_SIZE);
    }
    private Map<String, Integer> nextAvailablePositions = new HashMap<>();

    public void updateNextAvailablePosition(String fileName, int newPosition) {
        nextAvailablePositions.put(fileName, newPosition);
    }

    public int getNextAvailablePosition(String fileName) {
        return nextAvailablePositions.getOrDefault(fileName, Constants.BLOCK_SIZE);
    }

    /**
     * 파일 초기화 (새 레코드 삽입을 위한 헬퍼 메소드)
     */
    public void resetFile(String fileName) throws IOException {
        // 헤더 블록 초기화
        Block headerBlock = new Block();
        headerBlock.initializeHeaderBlock();
        writeBlock(fileName, 0, headerBlock);
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

        // 레코드 포인터 추적 (순환 감지용) - 배열 대신 Set 사용
        Set<Integer> visitedPointers = new HashSet<>();

        while (currentPointer >= 0) {
            // 순환 감지
            if (visitedPointers.contains(currentPointer)) {
                System.out.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers.add(currentPointer);

            try {
                // 레코드 읽기
                Record record = readRecord(fileName, currentPointer);
                if (record == null) {
                    break;
                }

                recordCount++;

                // 레코드 크기 직접 계산
                int recordSize = record.calculateRecordSize();
                totalRecordSize += recordSize;

                // 다음 레코드로 이동
                currentPointer = record.getNextPointer();
            } catch (Exception e) {
                System.err.println("통계 계산 중 오류: " + e.getMessage());
                break;
            }
        }

        // 평균 레코드 크기
        double avgRecordSize = recordCount > 0 ? (double) totalRecordSize / recordCount : 0;

        // blocking factor - 데이터 블록 당 평균 레코드 수
        double blockingFactor = avgRecordSize > 0 ? Constants.BLOCK_SIZE / avgRecordSize : 0;

        return new FileStats(fileSize, blockCount, recordCount, avgRecordSize, blockingFactor);
    }

//
//    /**
//     * 파일 통계 정보 가져오기
//     */
//    public FileStats getFileStats(String fileName) throws IOException {
//        String filePath = Constants.DATA_DIRECTORY + fileName;
//        File file = new File(filePath);
//
//        if (!file.exists()) {
//            throw new IOException("파일이 존재하지 않습니다: " + fileName);
//        }
//
//        // 전체 파일 크기 (바이트)
//        long fileSize = file.length();
//
//        // 블록 개수
//        int blockCount = (int) (fileSize / Constants.BLOCK_SIZE);
//
//        // 첫 레코드 포인터
//        int firstRecordPointer = getFirstRecordPointer(fileName);
//
//        // 레코드 개수와 평균 크기 계산
//        int recordCount = 0;
//        int totalRecordSize = 0;
//        int currentPointer = firstRecordPointer;
//
//        // 레코드 포인터 추적 (순환 감지용)
//        boolean[] visitedPointers = new boolean[10000];
//
//        while (currentPointer >= 0) {
//            // 순환 감지
//            if (visitedPointers[currentPointer]) {
//                break;
//            }
//            visitedPointers[currentPointer] = true;
//
//            try {
//                // 레코드 읽기
//                Record record = readRecord(fileName, currentPointer);
//                if (record == null) {
//                    break;
//                }
//
//                recordCount++;
//
//                // 다음 레코드 위치를 기준으로 현재 레코드 크기 추정
//                int nextPointer = record.getNextPointer();
//                if (nextPointer > currentPointer) {
//                    totalRecordSize += (nextPointer - currentPointer);
//                } else {
//                    // 마지막 레코드거나 다음 포인터가 이전 블록을 가리키는 경우
//                    // 레코드 직접 계산
//                    totalRecordSize += record.calculateRecordSize();
//                }
//
//                // 다음 레코드로 이동
//                currentPointer = nextPointer;
//            } catch (Exception e) {
//                break;
//            }
//        }
//
//        // 평균 레코드 크기
//        double avgRecordSize = recordCount > 0 ? (double) totalRecordSize / recordCount : 0;
//
//        // blocking factor
//        double blockingFactor = avgRecordSize > 0 ? Constants.BLOCK_SIZE / avgRecordSize : 0;
//
//        return new FileStats(fileSize, blockCount, recordCount, avgRecordSize, blockingFactor);
//    }

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