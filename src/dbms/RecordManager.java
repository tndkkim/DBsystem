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

        // 디스크 파일 초기화 (이전 데이터 삭제)
        try {
            resetFile(fileName);
            System.out.println("파일 초기화 완료");
        } catch (Exception e) {
            System.err.println("파일 초기화 중 오류 발생: " + e.getMessage());
            e.printStackTrace();
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

            // 모든 레코드를 먼저 파싱하고 정렬
            List<Record> records = new ArrayList<>();

            // 레코드 읽기
            String line;
            while ((line = reader.readLine()) != null && records.size() < recordCount) {
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
                        records.add(record); // 레코드 목록에 추가
                    } catch (Exception e) {
                        System.err.println("레코드 객체 생성 실패: " + e.getMessage());
                        e.printStackTrace();
                    }
                } catch (Exception e) {
                    System.err.println("레코드 처리 중 오류 발생: " + e.getMessage());
                    e.printStackTrace();
                }
            }

            // 검색 키(id)로 레코드 정렬
            records.sort((r1, r2) -> r1.getSearchKey().compareTo(r2.getSearchKey()));
            System.out.println("레코드 정렬 완료 - 총 " + records.size() + "개 레코드");

            // 정렬된 레코드 삽입 및 포인터 연결
            int prevPosition = -1;
            int firstPosition = -1;

            for (int i = 0; i < records.size(); i++) {
                Record record = records.get(i);

                // 레코드 위치 정하기 - writeRecord가 적절한 위치 선택
                int position = diskFileManager.writeRecord(fileName, record, 0);

                if (i == 0) {
                    // 첫 번째 레코드 위치 저장
                    firstPosition = position;
                }

                // 이전 레코드 포인터 업데이트
                if (prevPosition != -1) {
                    Record prevRecord = diskFileManager.readRecord(fileName, prevPosition);
                    prevRecord.setNextPointer(position);
                    diskFileManager.updateRecord(fileName, prevRecord, prevPosition);
                }

                // 다음 반복을 위해 현재 위치 저장
                prevPosition = position;

                System.out.println((i+1) + "번째 레코드 삽입 완료: " + record.getSearchKey() + ", 위치: " + position);
            }

            // 마지막 레코드의 다음 포인터를 -1로 설정
            if (prevPosition != -1) {
                Record lastRecord = diskFileManager.readRecord(fileName, prevPosition);
                lastRecord.setNextPointer(-1);
                diskFileManager.updateRecord(fileName, lastRecord, prevPosition);
                System.out.println("마지막 레코드 포인터 설정: -1");
            }

            // 첫 레코드 포인터 설정
            if (firstPosition != -1) {
                diskFileManager.setFirstRecordPointer(fileName, firstPosition);
                System.out.println("첫 레코드 포인터 설정: " + firstPosition);
            }

            try {
                DiskFileManager.FileStats stats = diskFileManager.getFileStats(fileName);
                System.out.println("\n===== 파일 통계 정보 =====");
                System.out.println(stats.toString());
                System.out.println("=======================");
            } catch (Exception e) {
                System.err.println("파일 통계 계산 중 오류 발생: " + e.getMessage());
            }

            System.out.println(records.size() + "개의 레코드가 성공적으로 삽입되었습니다.");
        } catch (IOException e) {
            System.err.println("파일 읽기 오류: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        System.out.println("===== bulkInsertRecords 종료 =====");
    }

    private void resetFile(String fileName) throws IOException {
        // 헤더 블록 초기화
        Block headerBlock = new Block();
        headerBlock.initializeHeaderBlock();
        diskFileManager.writeBlock(fileName, 0, headerBlock);

        // 다음 가용 위치 재설정
        diskFileManager.resetNextAvailablePosition();
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