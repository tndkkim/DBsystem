package dbms;

import dbms.util.ByteUtils;
import dbms.util.Constants;

import java.io.BufferedReader;
import java.io.File;
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
        System.out.println("===== bulkInsertRecords 시작 =====");

        // 파일 존재 확인 및 메타데이터 가져오기 (기존 코드와 동일)
        if (!metadataManager.fileExists(fileName)) {
            System.err.println("파일이 존재하지 않습니다: " + fileName);
            throw new IllegalArgumentException("파일이 존재하지 않습니다: " + fileName);
        }

        List<String> fieldNames = metadataManager.getFieldNames(fileName);
        List<Integer> fieldLengths = metadataManager.getFieldLengths(fileName);

        // 파일 초기화
        resetFile(fileName);

        try (BufferedReader reader = new BufferedReader(new FileReader(dataFilePath))) {
            // 파일 이름 및 레코드 수 라인 처리 (기존 코드와 동일)
            String fileNameLine = reader.readLine();
            String recordCountLine = reader.readLine();
            int expectedRecordCount = Integer.parseInt(recordCountLine);

            // 헤더 블록 초기화
            Block headerBlock = diskFileManager.readBlock(fileName, 0);
            headerBlock.setFirstRecordPointer(-1); // 초기에는 레코드 없음
            diskFileManager.writeBlock(fileName, 0, headerBlock);

            // 레코드를 하나씩 처리
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.trim().isEmpty()) continue;

                // 레코드 파싱
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

                // 누락된 필드 null로 처리
                while (fieldValues.size() < fieldNames.size()) {
                    fieldValues.add(null);
                }

                // Record 객체 생성
                Record record = new Record(fieldValues, fieldLengths, fieldNames);

                // 레코드를 순차적으로 삽입
                insertRecordSorted(fileName, record);
            }

            // 파일 통계 출력
            DiskFileManager.FileStats stats = diskFileManager.getFileStats(fileName);
            System.out.println("\n===== 파일 통계 정보 =====");
            System.out.println(stats.toString());

        } catch (IOException e) {
            System.err.println("파일 읽기 오류: " + e.getMessage());
            throw e;
        }

        System.out.println("===== bulkInsertRecords 종료 =====");
    }

    /**
     * 레코드를 search key 기준으로 정렬된 위치에 삽입
     */
    /**
     * 레코드를 search key 기준으로 정렬된 위치에 삽입
     */
    private void insertRecordSorted(String fileName, Record newRecord) throws IOException, SQLException {
        // 1. 헤더 블록에서 첫 레코드 포인터 가져오기
        Block headerBlock = diskFileManager.readBlock(fileName, 0);
        int firstRecordPointer = headerBlock.getFirstRecordPointer();

        // 새 레코드의 search key
        String newKey = newRecord.getSearchKey();

        // 2. 삽입 위치 찾기 (포인터 체인 순회)
        int prevPointer = -1; // 이전 레코드 포인터
        int currentPointer = firstRecordPointer; // 현재 레코드 포인터

        // 이미 레코드가 있는 경우
        if (currentPointer != -1) {
            boolean insertAtBeginning = false;

            // 첫 레코드보다 작은 경우 처리
            Record firstRecord = diskFileManager.readRecord(fileName, currentPointer);

            // NULL 체크 추가 - 첫 레코드를 읽지 못하면 첫 번째 위치에 삽입
            if (firstRecord == null) {
                System.out.println("첫 번째 레코드를 읽을 수 없습니다. 첫 번째 위치에 삽입합니다.");
                insertAtBeginning = true;
            }
            else if (newKey.compareTo(firstRecord.getSearchKey()) < 0) {
                insertAtBeginning = true;
            } else {
                // 적절한 위치 찾기
                while (currentPointer != -1) {
                    Record currentRecord = diskFileManager.readRecord(fileName, currentPointer);

                    // NULL 체크 추가 - 현재 레코드를 읽지 못하면 루프 종료
                    if (currentRecord == null) {
                        System.out.println("레코드를 읽을 수 없습니다: 포인터 " + currentPointer);
                        break;
                    }

                    int nextPointer = currentRecord.getNextPointer();

                    // 다음 레코드가 없거나, 다음 레코드보다 작은 경우
                    if (nextPointer == -1) {
                        // 다음 레코드가 없는 경우, 현재 레코드 다음에 삽입
                        break;
                    }

                    // 다음 레코드 읽기 (NULL 체크 추가)
                    Record nextRecord = diskFileManager.readRecord(fileName, nextPointer);
                    if (nextRecord == null) {
                        System.out.println("다음 레코드를 읽을 수 없습니다: 포인터 " + nextPointer);
                        break;
                    }

                    // 다음 레코드보다 작은 경우, 현재 레코드와 다음 레코드 사이에 삽입
                    if (nextRecord.getSearchKey().compareTo(newKey) > 0) {
                        break;
                    }

                    // 다음 레코드로 이동
                    prevPointer = currentPointer;
                    currentPointer = nextPointer;
                }
            }

            // 3. 새 레코드 저장 위치 계산
            int newRecordPosition = calculateNewRecordPosition(fileName, newRecord);

            // 4. 새 레코드 저장
            if (insertAtBeginning) {
                // 첫 레코드로 삽입
                newRecord.setNextPointer(firstRecordPointer);
                headerBlock.setFirstRecordPointer(newRecordPosition);
                diskFileManager.writeBlock(fileName, 0, headerBlock);
            } else {
                // 중간이나 끝에 삽입
                Record currentRecord = diskFileManager.readRecord(fileName, currentPointer);

                // NULL 체크 추가
                if (currentRecord == null) {
                    // 현재 레코드를 읽을 수 없으면 첫 레코드로 삽입
                    System.out.println("현재 레코드를 읽을 수 없습니다. 첫 레코드로 삽입합니다.");
                    newRecord.setNextPointer(firstRecordPointer);
                    headerBlock.setFirstRecordPointer(newRecordPosition);
                    diskFileManager.writeBlock(fileName, 0, headerBlock);
                } else {
                    newRecord.setNextPointer(currentRecord.getNextPointer());
                    currentRecord.setNextPointer(newRecordPosition);

                    // 현재 레코드 업데이트
                    saveRecord(fileName, currentRecord, currentPointer);
                }
            }

            // 새 레코드 저장
            saveRecord(fileName, newRecord, newRecordPosition);

        } else {
            // 첫 번째 레코드인 경우
            int newRecordPosition = calculateNewRecordPosition(fileName, newRecord);
            newRecord.setNextPointer(-1);

            // 헤더 블록 업데이트
            headerBlock.setFirstRecordPointer(newRecordPosition);
            diskFileManager.writeBlock(fileName, 0, headerBlock);

            // 레코드 저장
            saveRecord(fileName, newRecord, newRecordPosition);
        }
    }
    /**
     * 새 레코드 저장 위치 계산 (블록 단위 고려)
     */
    private int calculateNewRecordPosition(String fileName, Record record) throws IOException {
        int nextPosition = diskFileManager.getNextAvailablePosition(fileName);
        int blockNumber = nextPosition / Constants.BLOCK_SIZE;
        int blockOffset = nextPosition % Constants.BLOCK_SIZE;

        // 현재 레코드의 실제 크기 계산
        int recordSize = record.calculateRecordSize();

        // 블록 내 남은 공간 계산
        int freeSpace = Constants.BLOCK_SIZE - blockOffset;

        // 현재 레코드가 블록에 맞지 않는 경우
        if (freeSpace < recordSize) {
            // 다음 블록으로 이동
            blockNumber++;
            nextPosition = blockNumber * Constants.BLOCK_SIZE;
        }

        return nextPosition;
    }
    /**
     * 레코드를 지정된 위치에 저장 (블록 단위 I/O 고려)
     */
    private void saveRecord(String fileName, Record record, int position) throws IOException {
        int blockNumber = position / Constants.BLOCK_SIZE;
        int blockOffset = position % Constants.BLOCK_SIZE;

        // 레코드 직렬화
        byte[] recordBytes = record.toBytes();

        // 블록 읽기
        Block block;
        try {
            block = diskFileManager.readBlock(fileName, blockNumber);
        } catch (IOException e) {
            // 블록이 없으면 새 블록
            block = new Block();
        }

        // 블록에 레코드 추가
        System.arraycopy(recordBytes, 0, block.getData(), blockOffset, recordBytes.length);

        // 블록 쓰기
        diskFileManager.writeBlock(fileName, blockNumber, block);

        // 다음 가용 위치 업데이트
        diskFileManager.updateNextAvailablePosition(fileName, position + recordBytes.length);
    }


    public void debugPointerChain(String fileName) throws IOException, SQLException {
        System.out.println("\n===== 포인터 체인 디버깅 =====");

        // 헤더 블록에서 첫 번째 레코드 포인터 가져오기
        Block headerBlock = diskFileManager.readBlock(fileName, 0);
        int firstPointer = headerBlock.getFirstRecordPointer();

        System.out.println("첫 번째 레코드 포인터: " + firstPointer);

        // 방문한 포인터를 추적하여 순환 감지
        Set<Integer> visitedPointers = new HashSet<>();
        int currentPointer = firstPointer;
        int count = 0;

        // 그래픽 표현을 위한 StringBuilder
        StringBuilder chainVisual = new StringBuilder("헤더 [" + firstPointer + "] -> ");

        // 포인터 체인 따라가기
        while (currentPointer >= 0 && !visitedPointers.contains(currentPointer)) {
            visitedPointers.add(currentPointer);
            count++;

            try {
                // 현재 레코드 읽기
                Record record = diskFileManager.readRecord(fileName, currentPointer);

                if (record == null) {
                    System.out.println(count + ". 포인터: " + currentPointer + " -> 레코드를 읽을 수 없음");
                    chainVisual.append("NULL");
                    break;
                }

                int nextPointer = record.getNextPointer();
                String searchKey = record.getSearchKey();

                // 레코드 세부 정보와 다음 포인터 출력
                System.out.println(count + ". 포인터: " + currentPointer +
                        " | 키: " + searchKey +
                        " | 다음 포인터: " + nextPointer +
                        " | 블록: " + (currentPointer / Constants.BLOCK_SIZE) +
                        " | 오프셋: " + (currentPointer % Constants.BLOCK_SIZE));

                // 포인터 유효성 검사
                int fileSize = diskFileManager.getFileSize(fileName) * Constants.BLOCK_SIZE;
                if (nextPointer > 0 && (nextPointer >= fileSize || nextPointer < Constants.BLOCK_SIZE)) {
                    System.out.println("   [경고] 다음 포인터가 유효하지 않은 범위입니다: " + nextPointer);
                }

                // 체인 시각화 추가
                chainVisual.append("[").append(searchKey).append(":").append(currentPointer).append("]");
                if (nextPointer >= 0) {
                    chainVisual.append(" -> ");
                } else {
                    chainVisual.append(" -> NULL");
                }

                // 다음 포인터로 이동
                currentPointer = nextPointer;

                // 자기 참조 감지 (이중 확인)
                if (nextPointer == currentPointer && nextPointer >= 0) {
                    System.out.println("   [경고] 자기 참조 감지: 다음 포인터가 현재 포인터와 동일함");
                    break;
                }

            } catch (Exception e) {
                System.out.println(count + ". 포인터: " + currentPointer + " -> 예외 발생: " + e.getMessage());
                chainVisual.append("[ERROR]");
                break;
            }
        }

        if (visitedPointers.contains(currentPointer) && currentPointer >= 0) {
            System.out.println("순환 감지! 포인터 " + currentPointer + "가 이미 방문되었습니다.");
            chainVisual.append(" (순환 감지!)");
        }

        System.out.println("포인터 체인 시각화: " + chainVisual.toString());
        System.out.println("총 " + count + "개의 레코드가 체인에 연결되어 있습니다.");
        System.out.println("===== 포인터 체인 디버깅 종료 =====\n");
    }
    /**
     * 헤더 블록 정보를 디버깅하는 메서드
     */
    public void debugHeaderBlock(String fileName) throws IOException {
        try {
            Block headerBlock = diskFileManager.readBlock(fileName, 0);
            int firstRecordPointer = headerBlock.getFirstRecordPointer();

            System.out.println("  헤더 블록 위치: 0");
            System.out.println("  첫 번째 레코드 포인터: " + firstRecordPointer);

            if (firstRecordPointer >= 0) {
                try {
                    Record firstRecord = diskFileManager.readRecord(fileName, firstRecordPointer);
                    if (firstRecord != null) {
                        System.out.println("  첫 번째 레코드 정보:");
                        System.out.println("    검색 키: " + firstRecord.getSearchKey());
                        System.out.println("    다음 포인터: " + firstRecord.getNextPointer());
                        System.out.println("    물리적 위치: " + firstRecordPointer);
                        System.out.println("    블록 번호: " + (firstRecordPointer / Constants.BLOCK_SIZE));
                        System.out.println("    블록 내 오프셋: " + (firstRecordPointer % Constants.BLOCK_SIZE));
                    } else {
                        System.out.println("  첫 번째 레코드를 읽을 수 없습니다.");
                    }
                } catch (Exception e) {
                    System.out.println("  첫 번째 레코드 읽기 예외: " + e.getMessage());
                }
            } else {
                System.out.println("  파일에 레코드가 없습니다.");
            }
        } catch (Exception e) {
            System.err.println("헤더 블록 디버깅 중 오류 발생: " + e.getMessage());
        }
    }

    public void insertRecordIncremental(String fileName, Record newRecord) throws IOException, SQLException {
        System.out.println("===== insertRecordIncremental 시작 =====");

        // 1. 헤더 블록 읽기
        Block headerBlock = diskFileManager.readBlock(fileName, 0);
        int firstRecordPointer = headerBlock.getFirstRecordPointer();

        System.out.println("[DEBUG] 삽입 전 헤더 블록의 첫 번째 레코드 포인터: " + firstRecordPointer);

        // 새 레코드의 물리적 위치 계산 (파일 끝)
        int newRecordPosition = diskFileManager.getNextAvailablePosition(fileName);

        // 블록 번호와 오프셋 계산
        int blockNumber = newRecordPosition / Constants.BLOCK_SIZE;
        int blockOffset = newRecordPosition % Constants.BLOCK_SIZE;

        // 레코드 크기 계산
        newRecord.setNextPointer(-1); // 임시로 -1 설정
        byte[] recordBytes = newRecord.toBytes();
        int recordSize = recordBytes.length;

        System.out.println("새 레코드 " + newRecord.getSearchKey() +
                " 위치 " + newRecordPosition +
                " 크기 " + recordSize);

        // 현재 블록에 충분한 공간이 있는지 확인
        if (blockOffset + recordSize > Constants.BLOCK_SIZE) {
            // 블록이 가득 찼으므로 다음 블록으로 이동
            blockNumber++;
            blockOffset = 0;
            newRecordPosition = blockNumber * Constants.BLOCK_SIZE;
            System.out.println("블록이 가득 차서 다음 블록으로 이동: 새 위치 " + newRecordPosition);
        }

        // 2. 논리적 위치 찾기 (정렬 순서에 따라)
        String newKey = newRecord.getSearchKey();

        // 변수 초기화
        boolean isFirstRecord = false;
        int prevPointer = -1;
        Record prevRecord = null;
        int currentPointer = -1;

        if (firstRecordPointer < 0 ||
                (diskFileManager.readRecord(fileName, firstRecordPointer) != null &&
                        newKey.compareTo(diskFileManager.readRecord(fileName, firstRecordPointer).getSearchKey()) < 0)) {

            // 첫 번째 레코드로 설정
            isFirstRecord = true;
            if (firstRecordPointer < 0) {
                System.out.println("[DEBUG] 첫 번째 레코드 삽입: " + newKey);
                System.out.println("[DEBUG] 이전 첫 포인터: " + firstRecordPointer + " -> 새 첫 포인터: " + newRecordPosition);
            } else {
                Record firstRecord = diskFileManager.readRecord(fileName, firstRecordPointer);
                System.out.println("[DEBUG] 첫 번째 위치에 레코드 삽입: " + newKey);
                System.out.println("[DEBUG] 이전 첫 레코드: " + firstRecord.getSearchKey() + " (포인터: " + firstRecordPointer + ")");
                System.out.println("[DEBUG] 새 첫 레코드: " + newKey + " (포인터: " + newRecordPosition + ")");
                System.out.println("[DEBUG] 새 레코드의 다음 포인터: " + firstRecordPointer);
            }

            newRecord.setNextPointer(firstRecordPointer);
        } else {
            // 정렬된 위치 찾기
            prevPointer = firstRecordPointer;

            try {
                prevRecord = diskFileManager.readRecord(fileName, prevPointer);
                if (prevRecord != null) {
                    currentPointer = prevRecord.getNextPointer();

                    System.out.println("[DEBUG] 삽입 위치 탐색 시작:");
                    System.out.println("[DEBUG]   첫 레코드: " + prevRecord.getSearchKey() + " (포인터: " + prevPointer + ")");
                    System.out.println("[DEBUG]   첫 레코드의 다음 포인터: " + currentPointer);

                    // 적절한 위치 찾기
                    Set<Integer> visitedPointers = new HashSet<>(); // 순환 감지를 위한 집합
                    visitedPointers.add(prevPointer);

                    while (currentPointer >= 0) {
                        // 순환 감지
                        if (visitedPointers.contains(currentPointer)) {
                            System.out.println("[DEBUG] 순환 감지! 포인터 " + currentPointer + "가 이미 방문됨");
                            break;
                        }
                        visitedPointers.add(currentPointer);

                        Record currentRecord = diskFileManager.readRecord(fileName, currentPointer);
                        if (currentRecord == null) {
                            System.out.println("[DEBUG] 현재 포인터 " + currentPointer + "에서 레코드를 읽을 수 없음");
                            break;
                        }

                        System.out.println("[DEBUG]   현재 검사 중인 레코드: " + currentRecord.getSearchKey() + " (포인터: " + currentPointer + ")");
                        System.out.println("[DEBUG]   비교: 새 키 " + newKey + " vs 현재 키 " + currentRecord.getSearchKey());

                        // 현재 레코드보다 새 레코드가 작으면 여기에 삽입
                        // 또는 포인터 값이 이상한 경우(0 또는 음수)도 여기서 중단
                        if (newKey.compareTo(currentRecord.getSearchKey()) < 0 ||
                                currentRecord.getNextPointer() <= 0) {
                            System.out.println("[DEBUG]   삽입 위치 찾음: 새 키가 현재 키보다 작거나 체인의 끝임");
                            break;
                        }

                        // 다음으로 이동
                        prevPointer = currentPointer;
                        prevRecord = currentRecord;
                        currentPointer = currentRecord.getNextPointer();

                        // 포인터 유효성 검사
                        if (currentPointer < 0) {
                            System.out.println("[DEBUG]   체인의 끝에 도달 (다음 포인터: " + currentPointer + ")");
                            break;
                        }

                        System.out.println("[DEBUG]   다음 레코드로 이동: 이전 포인터 = " + prevPointer + ", 다음 포인터 = " + currentPointer);
                    }

                    // 이전 레코드 다음에 삽입
                    newRecord.setNextPointer(currentPointer);

                    System.out.println("[DEBUG] 중간/끝에 레코드 삽입:");
                    System.out.println("[DEBUG]   새 레코드: " + newKey + " (위치: " + newRecordPosition + ")");
                    System.out.println("[DEBUG]   이전 레코드: " + prevRecord.getSearchKey() + " (위치: " + prevPointer + ")");
                    System.out.println("[DEBUG]   다음 레코드 포인터: " + currentPointer);
                    System.out.println("[DEBUG]   포인터 연결: " + prevPointer + " -> " + newRecordPosition + " -> " + currentPointer);
                } else {
                    // 이전 레코드를 읽을 수 없는 경우, 첫 번째 위치에 삽입
                    isFirstRecord = true;
                    System.out.println("[DEBUG] 이전 레코드를 읽을 수 없어 첫 번째 위치에 삽입: " + newKey);
                    System.out.println("[DEBUG] 헤더 블록 첫 포인터 변경: " + firstRecordPointer + " -> " + newRecordPosition);

                    newRecord.setNextPointer(firstRecordPointer);
                }
            } catch (Exception e) {
                // 오류 발생 시 마지막에 추가
                System.err.println("정렬 위치 찾기 오류: " + e.getMessage() + " - 마지막에 추가합니다.");
                System.out.println("[DEBUG] 예외 발생으로 대체 처리: " + e.getMessage());

                if (prevRecord != null) {
                    newRecord.setNextPointer(-1); // 마지막 레코드는 -1을 가리키도록 명확히 설정
                    prevRecord.setNextPointer(newRecordPosition);
                    System.out.println("[DEBUG] 마지막에 추가: 이전 레코드 " + prevRecord.getSearchKey() +
                            " (위치: " + prevPointer + ") -> 새 레코드 " + newKey + " (위치: " + newRecordPosition + ")");
                } else {
                    // 이전 레코드가 없는 경우 첫 레코드로 설정
                    isFirstRecord = true;
                    newRecord.setNextPointer(-1);
                    System.out.println("[DEBUG] 이전 레코드가 없어 첫 레코드로 설정: " + newRecordPosition);
                }
            }
        }

        // 3. 레코드 저장 결정 - 블록 경계를 넘는 경우 처리
        int actualRecordPosition = newRecordPosition; // 실제로 레코드가 저장될 위치

        // 레코드 바이트 재생성 (포인터 설정 후)
        recordBytes = newRecord.toBytes();
        recordSize = recordBytes.length;

        // 레코드가 블록 내에 완전히 들어가는지 확인
        boolean needsBlockChange = false;
        if (blockOffset + recordSize > Constants.BLOCK_SIZE) {
            // 레코드가 블록 경계를 넘는 경우 다음 블록에 전체 저장
            System.out.println("레코드가 블록 경계를 넘습니다. 다음 블록의 처음부터 저장합니다.");

            // 다음 블록으로 이동
            blockNumber++;
            blockOffset = 0;
            actualRecordPosition = blockNumber * Constants.BLOCK_SIZE; // 실제 저장 위치 업데이트
            needsBlockChange = true;

            System.out.println("[DEBUG] 레코드 위치 변경: " + newRecordPosition + " -> " + actualRecordPosition);
        }

        // 4. 레코드 저장
        try {
            Block blockToWrite;
            try {
                blockToWrite = diskFileManager.readBlock(fileName, blockNumber);
            } catch (IOException e) {
                // 블록이 존재하지 않으면 새 블록 생성
                blockToWrite = new Block();
            }

            // 레코드 데이터 블록에 복사
            System.arraycopy(recordBytes, 0, blockToWrite.getData(), blockOffset, recordSize);

            // 블록 저장
            diskFileManager.writeBlock(fileName, blockNumber, blockToWrite);
            System.out.println("[DEBUG] 레코드 저장 완료: 블록 " + blockNumber + ", 오프셋 " + blockOffset);
        } catch (Exception e) {
            System.err.println("레코드 저장 중 오류 발생: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }

        // 5. 포인터 체인 업데이트
        // 위치가 변경되었다면 포인터 체인 업데이트
        if (needsBlockChange) {
            if (isFirstRecord) {
                // 첫 번째 레코드의 위치가 변경된 경우 헤더 블록 업데이트
                headerBlock.setFirstRecordPointer(actualRecordPosition);
                diskFileManager.writeBlock(fileName, 0, headerBlock);
                System.out.println("[DEBUG] 헤더 블록 업데이트: 첫 번째 레코드 포인터 변경 " + newRecordPosition + " -> " + actualRecordPosition);
            } else if (prevRecord != null) {
                // 중간/마지막 레코드의 위치가 변경된 경우 이전 레코드 업데이트
                prevRecord.setNextPointer(actualRecordPosition);

                // 이전 레코드 저장
                updatePreviousRecord(fileName, prevRecord, prevPointer);
            }
        } else {
            // 위치 변경이 없더라도 포인터 체인은 업데이트해야 함
            if (isFirstRecord) {
                // 첫 번째 레코드로 삽입하는 경우
                headerBlock.setFirstRecordPointer(actualRecordPosition);
                diskFileManager.writeBlock(fileName, 0, headerBlock);
                System.out.println("[DEBUG] 헤더 블록 업데이트: 첫 번째 레코드 포인터 = " + actualRecordPosition);
            } else if (prevRecord != null) {
                // 중간 또는 마지막에 삽입하는 경우
                prevRecord.setNextPointer(actualRecordPosition);

                // 이전 레코드 업데이트
                updatePreviousRecord(fileName, prevRecord, prevPointer);
            }
        }

        // 6. 다음 사용 가능 위치 업데이트
        diskFileManager.updateNextAvailablePosition(fileName, actualRecordPosition + recordSize);
        System.out.println("[DEBUG] 다음 사용 가능 위치 업데이트: " + (actualRecordPosition + recordSize));

        // 7. 삽입 후 헤더 블록 상태 확인
        Block updatedHeaderBlock = diskFileManager.readBlock(fileName, 0);
        int updatedFirstPointer = updatedHeaderBlock.getFirstRecordPointer();
        System.out.println("[DEBUG] 삽입 후 헤더 블록의 첫 번째 레코드 포인터: " + updatedFirstPointer);

        if (isFirstRecord && updatedFirstPointer != actualRecordPosition) {
            System.out.println("[DEBUG] 경고: 헤더 블록의 첫 포인터가 예상과 다릅니다!");
            System.out.println("[DEBUG]   예상: " + actualRecordPosition);
            System.out.println("[DEBUG]   실제: " + updatedFirstPointer);
        }

        System.out.println("레코드 " + newRecord.getSearchKey() + " 삽입 완료, 위치: " + actualRecordPosition);
        System.out.println("===== insertRecordIncremental 종료 =====");
    }

    /**
     * 이전 레코드의 다음 포인터를 업데이트하는 헬퍼 메서드
     */
    private void updatePreviousRecord(String fileName, Record prevRecord, int prevPointer) throws IOException {
        int prevBlockNumber = prevPointer / Constants.BLOCK_SIZE;
        int prevBlockOffset = prevPointer % Constants.BLOCK_SIZE;

        try {
            Block prevBlock = diskFileManager.readBlock(fileName, prevBlockNumber);
            byte[] prevRecordBytes = prevRecord.toBytes();

            // 이전 레코드가 블록 경계를 넘는지 확인
            if (prevBlockOffset + prevRecordBytes.length <= Constants.BLOCK_SIZE) {
                System.arraycopy(prevRecordBytes, 0, prevBlock.getData(), prevBlockOffset, prevRecordBytes.length);
                diskFileManager.writeBlock(fileName, prevBlockNumber, prevBlock);
                System.out.println("[DEBUG] 이전 레코드 업데이트 완료: 블록 " + prevBlockNumber + ", 오프셋 " + prevBlockOffset);
            } else {
                System.err.println("[경고] 이전 레코드가 블록 경계를 넘어 업데이트할 수 없습니다.");
                throw new IOException("이전 레코드가 블록 경계를 넘어 업데이트할 수 없습니다.");
            }
        } catch (Exception e) {
            System.err.println("이전 레코드 업데이트 중 오류 발생: " + e.getMessage());
            e.printStackTrace();
            throw e;
        }
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

        // 파일 크기 확인 (유효한 포인터 범위 확인용)
        long fileSize = 0;
        try {
            File file = new File(Constants.DATA_DIRECTORY + fileName);
            fileSize = file.length();
            System.out.println("파일 크기: " + fileSize + " 바이트");
        } catch (Exception e) {
            System.err.println("파일 크기 확인 중 오류: " + e.getMessage());
        }

        // 방문한 포인터를 기록하여 순환 감지
        Set<Integer> visitedPointers = new HashSet<>();

        int recordCount = 0;
        int maxRecords = 1000; // 안전장치: 무한 루프 방지

        // 모든 레코드 순회
        while (currentPointer >= 0 && recordCount < maxRecords) {
            recordCount++;
            System.out.println("레코드 #" + recordCount + " 처리 - 포인터: " + currentPointer);

            // 포인터 유효성 검사
            if (currentPointer < 0 || currentPointer >= fileSize) {
                System.err.println("포인터가 유효하지 않은 범위에 있습니다: " + currentPointer + ", 파일 크기: " + fileSize);
                break;
            }

            // 순환 감지
            if (visitedPointers.contains(currentPointer)) {
                System.err.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers.add(currentPointer);

            // 레코드 읽기
            Record record = null;
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

            System.out.println("레코드 읽기 성공: " + record.toString());

            // 필드 값 추가
            String fieldValue = record.getFieldValue(fieldName);
            results.add(fieldValue);
            System.out.println("필드 값 추가: " + fieldValue);

            // 다음 레코드로 이동
            int nextPointer = record.getNextPointer();
            System.out.println("다음 레코드 포인터: " + nextPointer);

            // 포인터 유효성 검사
            if (nextPointer < -1) {
                System.err.println("잘못된 다음 포인터 값 (음수): " + nextPointer);
                break;
            }

            if (nextPointer > 0 && nextPointer >= fileSize) {
                System.err.println("잘못된 다음 포인터 값 (파일 크기 초과): " + nextPointer + ", 파일 크기: " + fileSize);
                break;
            }

            // 자기 참조 감지
            if (nextPointer == currentPointer && nextPointer != -1) {
                System.err.println("무한 루프 감지: 다음 포인터가 현재 포인터와 동일함");
                break;
            }

            currentPointer = nextPointer;
        }

        // 최대 레코드 수 초과 검사
        if (recordCount >= maxRecords) {
            System.err.println("최대 레코드 수(" + maxRecords + ")를 초과하여 검색을 중단합니다.");
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

        // 검색 전 포인터 체인 디버깅
        System.out.println("\n[DEBUG-검색전] 레코드 검색 전 포인터 체인:");
        debugPointerChain(fileName);

        // 방문한 포인터를 기록하여 순환 감지
        Set<Integer> visitedPointers = new HashSet<>();

        int recordCount = 0;
        String fileSize = "0";

        try {
            // 파일 크기 확인
            File file = new File(Constants.DATA_DIRECTORY + fileName);
            fileSize = String.valueOf(file.length());
        } catch (Exception e) {
            System.err.println("파일 크기 확인 중 오류: " + e.getMessage());
        }

        // 모든 레코드 순회
        while (currentPointer >= 0) {
            recordCount++;
            System.out.println("레코드 #" + recordCount + " 처리 - 포인터: " + currentPointer);

            // 포인터 유효성 검사
            if (currentPointer < 0 || currentPointer >= Integer.MAX_VALUE / 2) {
                System.err.println("잘못된 포인터 값: " + currentPointer + ", 파일 크기: " + fileSize);
                break;
            }

            // 순환 감지
            if (visitedPointers.contains(currentPointer)) {
                System.err.println("순환 감지: 이미 방문한 포인터 " + currentPointer);
                break;
            }
            visitedPointers.add(currentPointer);

            // 레코드 읽기
            Record record = null;
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

            // 포인터 유효성 검사
            if (nextPointer < -1 || nextPointer >= Integer.MAX_VALUE / 2) {
                System.err.println("잘못된 다음 포인터 값: " + nextPointer + ", 파일 크기: " + fileSize);
                break;
            }

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

    public void debugPointerChainDetailed(String fileName) throws IOException, SQLException {
        System.out.println("\n===== 포인터 체인 상세 디버깅 =====");

        // 헤더 블록에서 첫 번째 레코드 포인터 가져오기
        Block headerBlock = diskFileManager.readBlock(fileName, 0);
        int firstPointer = headerBlock.getFirstRecordPointer();

        System.out.println("첫 번째 레코드 포인터: " + firstPointer);

        // 포인터 크기 출력
        System.out.println("포인터 크기(Constants.POINTER_SIZE): " + Constants.POINTER_SIZE + " 바이트");

        // 방문한 포인터를 추적하여 순환 감지
        Set<Integer> visitedPointers = new HashSet<>();
        int currentPointer = firstPointer;
        int count = 0;

        // 그래픽 표현을 위한 StringBuilder
        StringBuilder chainVisual = new StringBuilder("헤더 [" + firstPointer + "] -> ");

        // 포인터 체인 따라가기
        while (currentPointer >= 0 && !visitedPointers.contains(currentPointer)) {
            visitedPointers.add(currentPointer);
            count++;

            try {
                // 현재 레코드 읽기
                Record record = diskFileManager.readRecord(fileName, currentPointer);

                if (record == null) {
                    System.out.println(count + ". 포인터: " + currentPointer + " -> 레코드를 읽을 수 없음");
                    chainVisual.append("NULL");
                    break;
                }

                int nextPointer = record.getNextPointer();
                String searchKey = record.getSearchKey();

                // 레코드 세부 정보와 다음 포인터 출력
                System.out.println(count + ". 포인터: " + currentPointer +
                        " | 키: " + searchKey +
                        " | 다음 포인터: " + nextPointer +
                        " | 블록: " + (currentPointer / Constants.BLOCK_SIZE) +
                        " | 오프셋: " + (currentPointer % Constants.BLOCK_SIZE));

                // 포인터의 바이트 표현 출력
                byte[] pointerBytes = ByteUtils.intToBytes(nextPointer);
                System.out.print("   포인터 바이트 표현: [");
                for (int i = 0; i < pointerBytes.length; i++) {
                    System.out.print(String.format("%d", pointerBytes[i]));
                    if (i < pointerBytes.length - 1) {
                        System.out.print(", ");
                    }
                }
                System.out.println("]");

                // 레코드 크기 정보 출력
                int recordSize = record.calculateRecordSize();
                byte[] recordBytes = record.toBytes();
                System.out.println("   계산된 레코드 크기: " + recordSize + " 바이트");
                System.out.println("   실제 레코드 바이트 길이: " + recordBytes.length + " 바이트");

                // null 비트맵 정보 출력
                byte nullBitmap = recordBytes[0];
                System.out.println("   Null 비트맵: " + String.format("0x%02X", nullBitmap) +
                        " (" + Integer.toBinaryString(nullBitmap & 0xFF) + ")");

                // 체인 시각화 추가
                chainVisual.append("[").append(searchKey).append(":").append(currentPointer).append("]");
                if (nextPointer >= 0) {
                    chainVisual.append(" -> ");
                } else {
                    chainVisual.append(" -> NULL");
                }

                // 포인터 유효성 검사
                if (nextPointer < -1 || nextPointer > diskFileManager.getFileSize(fileName)) {
                    System.out.println("   [경고] 다음 포인터가 유효하지 않은 범위입니다: " + nextPointer);
                }

                // 다음 포인터로 이동
                currentPointer = nextPointer;

            } catch (Exception e) {
                System.out.println(count + ". 포인터: " + currentPointer + " -> 예외 발생: " + e.getMessage());
                e.printStackTrace();
                chainVisual.append("[ERROR]");
                break;
            }
        }

        if (visitedPointers.contains(currentPointer) && currentPointer != -1) {
            System.out.println("순환 감지! 포인터 " + currentPointer + "가 이미 방문되었습니다.");
            chainVisual.append(" (순환 감지!)");
        }

        System.out.println("포인터 체인 시각화: " + chainVisual.toString());
        System.out.println("총 " + count + "개의 레코드가 체인에 연결되어 있습니다.");
        System.out.println("===== 포인터 체인 상세 디버깅 종료 =====\n");
    }

    /**
     * Record 객체의 바이트 표현을 자세히 출력하는 메서드
     */
    public void printRecordBytes(Record record) {
        System.out.println("\n===== 레코드 바이트 상세 정보 =====");

        byte[] bytes = record.toBytes();
        int recordSize = record.calculateRecordSize();

        System.out.println("레코드 키: " + record.getSearchKey());
        System.out.println("다음 포인터 값: " + record.getNextPointer());
        System.out.println("계산된 레코드 크기: " + recordSize + " 바이트");
        System.out.println("실제 바이트 배열 길이: " + bytes.length + " 바이트");

        // Null 비트맵 출력
        byte nullBitmap = bytes[0];
        System.out.println("Null 비트맵 (첫 바이트): " + String.format("0x%02X", nullBitmap) +
                " (" + Integer.toBinaryString(nullBitmap & 0xFF) + ")");

        // 필드 데이터 부분 (null이 아닌 필드만)
        System.out.println("필드 데이터 (바이트 1부터 " + (bytes.length - Constants.POINTER_SIZE - 1) + "):");
        for (int i = 1; i < bytes.length - Constants.POINTER_SIZE; i++) {
            System.out.print(String.format("%02X ", bytes[i]));
            if ((i - 1) % 16 == 15) System.out.println();
        }
        System.out.println();

        // 포인터 부분
        System.out.println("포인터 부분 (마지막 " + Constants.POINTER_SIZE + " 바이트):");
        for (int i = bytes.length - Constants.POINTER_SIZE; i < bytes.length; i++) {
            System.out.print(String.format("%02X ", bytes[i]));
        }
        System.out.println("\n===== 레코드 바이트 상세 정보 종료 =====");
    }

    /**
     * 주어진 파일이름과 레코드 키를 이용해 해당 레코드의 바이트를 출력
     */
    public void printRecordBytesByKey(String fileName, String key) throws IOException, SQLException {
        System.out.println("\n===== 레코드 '" + key + "' 바이트 검색 =====");

        // 첫 레코드부터 순회하면서 해당 키를 가진 레코드 찾기
        int currentPointer = diskFileManager.getFirstRecordPointer(fileName);

        while (currentPointer >= 0) {
            Record record = diskFileManager.readRecord(fileName, currentPointer);
            if (record == null) {
                System.out.println("레코드를 읽을 수 없음: 포인터 " + currentPointer);
                break;
            }

            if (record.getSearchKey().equals(key)) {
                System.out.println("레코드 '" + key + "' 찾음! 포인터: " + currentPointer);
                printRecordBytes(record);
                return;
            }

            currentPointer = record.getNextPointer();
        }

        System.out.println("키가 '" + key + "'인 레코드를 찾을 수 없습니다.");
        System.out.println("===== 레코드 바이트 검색 종료 =====");
    }

}