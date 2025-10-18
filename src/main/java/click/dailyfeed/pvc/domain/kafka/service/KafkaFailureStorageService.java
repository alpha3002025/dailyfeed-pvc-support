package click.dailyfeed.pvc.domain.kafka.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class KafkaFailureStorageService {
    private final DateTimeFormatter dir_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private final DateTimeFormatter file_formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd-hh-mm-ss");

    private static final String rootDir = "/kafka-failures";

    public String createUniqueFilePath(String targetService, String eventType, String key){
        String uuid = UUID.randomUUID().toString().substring(0, 8);
        LocalDateTime now = LocalDateTime.now();
        /// /kafka-failures/{target-service}/{eventType}/yyyyMMdd
        return new StringBuffer(rootDir) // 추후 변수로 지정 예정. 현재는 용도가 많지 않아서 하드코딩
                .append("/").append(targetService)
                .append("/").append(eventType)
                .append("/").append(now.format(dir_formatter)) /// /{eventType}/yyyyMMdd
                .append("/").append(now.format(file_formatter)).append("---").append(key).append("---").append(uuid) /// yyyy-MM-dd-hh-mm-ss---{RedisKey}---{uuid}
                .toString();
    }

    public String store(String targetService, String eventType, String key, String payload) throws IOException {
        // 파일 경로 생성
        String baseFilePath = createUniqueFilePath(targetService, eventType, key);
        String fullFilePath = baseFilePath + ".json";

        Path filePath = Paths.get(fullFilePath);
        Path dirPath = filePath.getParent();

        // 디렉터리 생성 (존재하지 않는 경우)
        if (!Files.exists(dirPath)) {
            Files.createDirectories(dirPath);
            log.info("Created directory: {}", dirPath);
        }

        // JSON 파일 생성 및 payload 기록 (try-with-resources로 리소스 자동 반환)
        try (BufferedWriter writer = Files.newBufferedWriter(
                filePath,
                StandardCharsets.UTF_8,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            writer.write(payload);
            writer.flush();
        }

        log.info("Created failure file with payload: {}", fullFilePath);

        return fullFilePath;
    }

}
