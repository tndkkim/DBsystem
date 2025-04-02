package dbms;

import dbms.util.Constants;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class RecordManager {
    private DiskFileManager diskFileManager;
    private MetadataManager metadataManager;

    public RecordManager(DiskFileManager diskFileManager, MetadataManager metadataManager) {
        this.diskFileManager = diskFileManager;
        this.metadataManager = metadataManager;
    }

    /**
     * 레코드 일괄 삽입
     */
    public void bulkInsertRecords(String fileName, String dataFilePath) throws IOException, SQLException {
        System.out.println("===== bulkInsertRecords 시작 =====");

        // 파일이 존재하는지 확인
        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        // 메타데이터 가져오기
        List<String> fieldNames;
        List<Integer> fieldLengths;
        try {
            fieldNames = metadataManager.getFieldNames(fileName);
            fieldLengths = metadataManager.getFieldLengths(fileName);
            System.out.println("메타데이터 로드 성공 - 필드 수: " + fieldNames.size());
        } catch (Exception e) {
            System.err.println("메타데이터 로드 실패: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // 데이터 파일 읽기
        try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {
            System.out.println("데이터 파일 열기 성공: " + dataFilePath);

            // 첫 줄은 파일 이름
            String fileNameLine = reader.readLine();
            System.out.println("파일 이름 라인: " + fileNameLine);

            // 두 번째 줄은 레코드 수
            String recordCountLine = reader.readLine();
            int recordCount = Integer.parseInt(recordCountLine);
            System.out.println("레코드 수 라인: " + recordCountLine + " (숫자: " + recordCount + ")");

            System.out.println("파일: " + fileNameLine + ", 삽입할 레코드 수: " + recordCount);

            // 레코드 읽기 및 삽입 - 한 줄씩 처리
            String line;
            int recordCounter = 0;

            while ((line = reader.readLine()) != null && recordCounter < recordCount) {
                // 빈 줄 무시
                if (line.trim().isEmpty()) {
                    System.out.println("빈 줄 무시됨");
                    continue;
                }

                System.out.println("처리중인 라인: " + line);

                try {
                    // 레코드 파싱
                    String[] values = line.split(Constants.DELIMITER);
                    System.out.println("필드 개수: " + values.length);

                    // 필드 값 목록 생성
                    List<String> fieldValues = new ArrayList<>();
                    for (int i = 0; i < values.length; i++) {
                        String value = values[i].trim();
                        if (Constants.NULL_VALUE.equalsIgnoreCase(value)) {
                            fieldValues.add(null);
                            System.out.println("필드 " + i + ": null");
                        } else {
                            fieldValues.add(value);
                            System.out.println("필드 " + i + ": " + value);
                        }
                    }

                    // 누락된 필드 처리
                    while (fieldValues.size() < fieldNames.size()) {
                        fieldValues.add(null);
                        System.out.println("누락된 필드 추가: null");
                    }

                    // Record 객체 생성
                    Record record;
                    try {
                        record = new Record(fieldValues, fieldLengths, fieldNames);
                        System.out.println("레코드 객체 생성 완료: " + record.toString());
                    } catch (Exception e) {
                        System.err.println("레코드 객체 생성 실패: " + e.getMessage());
                        e.printStackTrace();
                        continue;
                    }

                    // 순차 파일에 적절한 위치에 레코드 삽입 - 블록 I/O 사용
                    try {
                        insertRecordInOrder(fileName, record);
                        System.out.println("레코드 삽입 완료: " + record.getSearchKey());
                    } catch (Exception e) {
                        System.err.println("레코드 삽입 중 오류 발생: " + e.getMessage());
                        e.printStackTrace();
                    }

                    recordCounter++;
                    System.out.println(recordCounter + "번째 레코드 처리 완료");
                } catch (Exception e) {
                    System.err.println("레코드 처리 중 오류 발생: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // 파일 통계 출력
            try {
                DiskFileManager.FileStats stats = diskFileManager.getFileStats(fileName);
                System.out.println("\n===== 파일 통계 정보 =====");
                System.out.println(stats.toString());
                System.out.println("=======================");
            } catch (Exception e) {
                System.err.println("파일 통계 계산 중 오류 발생: " + e.getMessage());
            }

            System.out.println(recordCounter + "개의 레코드가 성공적으로 삽입되었습니다.");

        } catch (IOException e) {
            System.err.println("파일 읽기 오류: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        System.out.println("===== bulkInsertRecords 종료 =====");
    }

    /**
     * 순차 파일에 순서대로 레코드 삽입 (블록 I/O 이용)
     */
    private void insertRecordInOrder(String fileName, Record newRecord) throws IOException, SQLException {
        System.out.println("===== insertRecordInOrder 시작 =====");

        // 새 레코드의 검색 키
        String newKey = newRecord.getSearchKey();
        System.out.println("새 레코드 검색 키: " + newKey);

        // 헤더 블록 읽기 (블록 I/O)
        Block headerBlock = diskFileManager.readBlock(fileName, 0);
        int firstRecordPointer = headerBlock.getFirstRecordPointer();
        System.out.println("첫 레코드 포인터: " + firstRecordPointer);

        // 각 레코드마다 새로운 블록 번호 계산 (기존 블록 개수 + 1)
        int blockCount = diskFileManager.getFileSize(fileName);
        int newBlockNumber = blockCount; // 새 블록 번호
        int newPosition = newBlockNumber * Constants.BLOCK_SIZE; // 새 위치

        // 파일이 비어있는 경우
        if (firstRecordPointer < 0) {
            System.out.println("파일이 비어있음 - 첫 번째 레코드 추가");

            // 새 블록 준비 (블록 I/O)
            Block dataBlock = new Block();

            // 레코드를 바이트 배열로 변환
            byte[] recordBytes = newRecord.toBytes();

            // 블록에 레코드 데이터 복사 (블록의 시작 부분에)
            System.arraycopy(recordBytes, 0, dataBlock.getData(), 0, recordBytes.length);

            // 블록 쓰기 (블록 I/O)
            diskFileManager.writeBlock(fileName, newBlockNumber, dataBlock);
            System.out.println("레코드 추가됨, 새 위치: " + newPosition);

            // 헤더 블록의 첫 레코드 포인터 업데이트
            headerBlock.setFirstRecordPointer(newPosition);
            diskFileManager.writeBlock(fileName, 0, headerBlock);
            System.out.println("첫 레코드 포인터 업데이트됨: " + newPosition);

            System.out.println("===== insertRecordInOrder 종료 (첫 레코드) =====");
            return;
        }

        // 첫 번째 레코드 읽기
        Record firstRecord = diskFileManager.readRecord(fileName, firstRecordPointer);
        if (firstRecord == null) {
            System.err.println("첫 번째 레코드 읽기 실패: null 반환됨");
            // 첫 레코드가 null인 경우, 파일 초기화 후 첫 레코드로 삽입
            resetFile(fileName);
            insertRecordInOrder(fileName, newRecord);
            return;
        }
        System.out.println("첫 번째 레코드 읽기 성공: " + firstRecord.toString());

        // 첫 번째 레코드보다 작은 경우 - 첫 위치에 삽입
        if (newKey.compareTo(firstRecord.getSearchKey()) < 0) {
            System.out.println("새 레코드가 첫 번째 레코드보다 작음 - 첫 번째 위치에 삽입");

            // 새 블록 준비 (블록 I/O)
            Block dataBlock = new Block();

            // 레코드가 첫 번째가 되므로 다음 포인터를 기존 첫 레코드로 설정
            newRecord.setNextPointer(firstRecordPointer);

            // 레코드를 바이트 배열로 변환
            byte[] recordBytes = newRecord.toBytes();

            // 블록에 레코드 데이터 복사
            System.arraycopy(recordBytes, 0, dataBlock.getData(), 0, recordBytes.length);

            // 블록 쓰기 (블록 I/O)
            diskFileManager.writeBlock(fileName, newBlockNumber, dataBlock);
            System.out.println("레코드 추가됨, 새 위치: " + newPosition);

            // 헤더 블록의 첫 레코드 포인터 업데이트
            headerBlock.setFirstRecordPointer(newPosition);
            diskFileManager.writeBlock(fileName, 0, headerBlock);
            System.out.println("첫 레코드 포인터 업데이트됨: " + newPosition);

            System.out.println("===== insertRecordInOrder 종료 (첫 위치 삽입) =====");
            return;
        }

        // 삽입 위치 찾기
        System.out.println("삽입 위치 찾기 시작");

        int currentPointer = firstRecordPointer;
        int prevPointer = -1;
        int loopCount = 0;
        int maxLoopCount = 100; // 무한 루프 방지

        boolean[] visitedPointers = new boolean[10000]; // 방문한 포인터 기록 (순환 감지)
        Record currentRecord = firstRecord;
        Record prevRecord = null;

        while (currentPointer >= 0 && loopCount < maxLoopCount) {
            loopCount++;
            System.out.println("루프 " + loopCount + " - 현재 포인터: " + currentPointer + ", 이전 포인터: " + prevPointer);

            // 순환 감지
            if (visitedPointers[currentPointer]) {
                System.err.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers[currentPointer] = true;

            String currentKey = currentRecord.getSearchKey();
            System.out.println("현재 레코드 키: " + currentKey + ", 새 레코드 키: " + newKey);

            // 현재 레코드보다 크거나 같으면 중단
            if (newKey.compareTo(currentKey) <= 0) {
                System.out.println("삽입 위치 찾음: 새 레코드 키가 현재 레코드 키보다 작거나 같음");
                break;
            }

            // 다음 레코드 위치 가져오기
            int nextPointer = currentRecord.getNextPointer();
            System.out.println("다음 레코드 포인터: " + nextPointer);

            // 자기 참조 감지
            if (nextPointer == currentPointer) {
                System.err.println("자기 참조 감지: 현재 포인터와 다음 포인터가 동일함");
                // 다음 포인터를 -1로 설정 (마지막 레코드로 만듦)
                currentRecord.setNextPointer(-1);

                // 해당 블록 번호와 오프셋 계산
                int blockNumber = currentPointer / Constants.BLOCK_SIZE;
                int blockOffset = currentPointer % Constants.BLOCK_SIZE;

                // 블록 읽기 (블록 I/O)
                Block block = diskFileManager.readBlock(fileName, blockNumber);

                // 수정된 레코드 데이터 저장
                byte[] recordBytes = currentRecord.toBytes();
                System.arraycopy(recordBytes, 0, block.getData(), blockOffset, recordBytes.length);

                // 블록 쓰기 (블록 I/O)
                diskFileManager.writeBlock(fileName, blockNumber, block);

                nextPointer = -1;
            }

            // 마지막 레코드에 도달한 경우
            if (nextPointer < 0) {
                break;
            }

            // 다음 레코드로 이동
            prevPointer = currentPointer;
            prevRecord = currentRecord;
            currentPointer = nextPointer;

            try {
                // 다음 레코드 읽기
                currentRecord = diskFileManager.readRecord(fileName, currentPointer);
                if (currentRecord == null) {
                    System.err.println("레코드 읽기 실패: null 반환됨");
                    break;
                }
                System.out.println("다음 레코드 읽기 성공: " + currentRecord.toString());
            } catch (Exception e) {
                System.err.println("레코드 읽기 오류: " + e.getMessage());
                break;
            }
        }

        if (loopCount >= maxLoopCount) {
            System.err.println("잠재적인 무한 루프 감지: 최대 반복 횟수 초과");
        }

        // 삽입 위치에 레코드 추가
        if (prevPointer < 0) {
            // 첫 번째 레코드로 삽입 (이미 처리됨)
            System.out.println("첫 번째 위치에 삽입 (이미 처리됨)");
            System.out.println("===== insertRecordInOrder 종료 (첫 위치 중복) =====");
            return;
        } else {
            // 중간이나 마지막에 삽입
            System.out.println("중간 또는 마지막 위치에 삽입");

            // 새 블록 준비 (블록 I/O)
            Block dataBlock = new Block();

            // 새 레코드의 다음 포인터 설정
            newRecord.setNextPointer(currentPointer);

            // 레코드를 바이트 배열로 변환
            byte[] recordBytes = newRecord.toBytes();

            // 블록에 레코드 데이터 복사
            System.arraycopy(recordBytes, 0, dataBlock.getData(), 0, recordBytes.length);

            // 블록 쓰기 (블록 I/O)
            diskFileManager.writeBlock(fileName, newBlockNumber, dataBlock);
            System.out.println("레코드 추가됨, 새 위치: " + newPosition);

            // 이전 레코드의 다음 포인터 업데이트
            prevRecord.setNextPointer(newPosition);

            // 이전 레코드 블록 번호와 오프셋 계산
            int prevBlockNumber = prevPointer / Constants.BLOCK_SIZE;
            int prevBlockOffset = prevPointer % Constants.BLOCK_SIZE;

            // 이전 레코드 블록 읽기 (블록 I/O)
            Block prevBlock = diskFileManager.readBlock(fileName, prevBlockNumber);

            // 수정된 이전 레코드 데이터 저장
            byte[] prevRecordBytes = prevRecord.toBytes();
            System.arraycopy(prevRecordBytes, 0, prevBlock.getData(), prevBlockOffset, prevRecordBytes.length);

            // 블록 쓰기 (블록 I/O)
            diskFileManager.writeBlock(fileName, prevBlockNumber, prevBlock);
            System.out.println("이전 레코드의 다음 포인터 업데이트됨: " + newPosition);
        }

        System.out.println("===== insertRecordInOrder 종료 (중간 또는 마지막 위치) =====");
    }



    /**
     * 파일 초기화 (이전 데이터 삭제)
     */
    private void resetFile(String fileName) throws IOException {
        // 헤더 블록 초기화
        diskFileManager.resetFile(fileName);
        System.out.println("파일 초기화 완료");
    }

    public List<String> searchField(String fileName, String fieldName) throws IOException, SQLException {
        System.out.println("===== searchField 시작 =====");

        // 파일이 존재하는지 확인
        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        // 필드 인덱스 가져오기
        int fieldIndex = metadataManager.getFieldIndex(fileName, fieldName);
        if (fieldIndex == -1) {
            System.err.println("존재하지 않는 필드: " + fieldName);
            throw new IllegalArgumentException("존재하지 않는 필드: " + fieldName);
        }

        System.out.println("필드 검색: " + fieldName + ", 인덱스: " + fieldIndex);

        // 결과 목록 초기화
        List<String> results = new ArrayList<>();

        // 첫 레코드 포인터 가져오기
        int currentPointer = diskFileManager.getFirstRecordPointer(fileName);
        System.out.println("첫 레코드 포인터: " + currentPointer);

        // 방문한 포인터를 기록하여 순환 감지
        boolean[] visitedPointers = new boolean[10000];

        int recordCount = 0;
        // 모든 레코드 순회
        while (currentPointer >= 0) {
            recordCount++;
            System.out.println("레코드 #" + recordCount + " 처리 - 포인터: " + currentPointer);

            // 순환 감지
            if (visitedPointers[currentPointer]) {
                System.err.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers[currentPointer] = true;

            // 레코드 읽기
            Record record = diskFileManager.readRecord(fileName, currentPointer);
            if (record == null) {
                System.err.println("레코드 읽기 실패: null 반환됨");
                break;
            }

            System.out.println("레코드 읽기 성공: " + record.toString());

            // 필드 값 추가
            String fieldValue = record.getFieldValue(fieldName);
            results.add(fieldValue);
            System.out.println("필드 값 추가: " + fieldValue);

            // 다음 레코드로 이동
            int nextPointer = record.getNextPointer();
            System.out.println("다음 레코드 포인터: " + nextPointer);

            // 자기 참조 감지
            if (nextPointer == currentPointer) {
                System.err.println("무한 루프 감지: 다음 포인터가 현재 포인터와 동일함");
                break;
            }

            currentPointer = nextPointer;
        }

        System.out.println("검색 결과 수: " + results.size());
        System.out.println("===== searchField 종료 =====");

        return results;
    }

    public List<Record> searchRecords(String fileName, String minKey, String maxKey) throws IOException, SQLException {
        System.out.println("===== searchRecords 시작 =====");
        System.out.println("검색 조건: " + minKey + " <= 키 <= " + maxKey);

        // 파일이 존재하는지 확인
        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        // 결과 목록 초기화
        List<Record> results = new ArrayList<>();

        // 첫 레코드 포인터 가져오기
        int currentPointer = diskFileManager.getFirstRecordPointer(fileName);
        System.out.println("첫 레코드 포인터: " + currentPointer);

        // 방문한 포인터를 기록하여 순환 감지
        boolean[] visitedPointers = new boolean[10000];

        int recordCount = 0;
        // 모든 레코드 순회
        while (currentPointer >= 0) {
            recordCount++;
            System.out.println("레코드 #" + recordCount + " 처리 - 포인터: " + currentPointer);

            // 순환 감지
            if (visitedPointers[currentPointer]) {
                System.err.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers[currentPointer] = true;

            // 레코드 읽기
            Record record = diskFileManager.readRecord(fileName, currentPointer);
            if (record == null) {
                System.err.println("레코드 읽기 실패: null 반환됨");
                break;
            }

            System.out.println("레코드 읽기 성공: " + record.toString());

            // 검색 키 비교
            String key = record.getSearchKey();
            System.out.println("레코드 키: " + key);

            // 최소 키보다 크거나 같고 최대 키보다 작거나 같은 경우
            if (key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0) {
                results.add(record);
                System.out.println("결과에 레코드 추가됨: " + key);
            }

            // 최대 키보다 큰 경우 중단
            if (key.compareTo(maxKey) > 0) {
                System.out.println("최대 키보다 큼, 검색 종료");
                break;
            }

            // 다음 레코드로 이동
            int nextPointer = record.getNextPointer();
            System.out.println("다음 레코드 포인터: " + nextPointer);

            // 자기 참조 감지
            if (nextPointer == currentPointer) {
                System.err.println("무한 루프 감지: 다음 포인터가 현재 포인터와 동일함");
                break;
            }

            currentPointer = nextPointer;
        }

        System.out.println("검색 결과 수: " + results.size());
        System.out.println("===== searchRecords 종료 =====");

        return results;
    }
}