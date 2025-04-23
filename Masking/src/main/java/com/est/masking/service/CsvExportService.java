package com.est.masking.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

@Service
public class CsvExportService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${csv.export.directory:C:/csv-files}")
    private String exportDirectory;

    public String exportTableToCsv(String tableName) {
        String sql = "SELECT * FROM " + tableName;
        List<Map<String, Object>> rows = jdbcTemplate.queryForList(sql);

        if (rows.isEmpty()) {
            return "No data found for table: " + tableName;
        }

        File dir = new File(exportDirectory);
        if (!dir.exists()) {
            dir.mkdirs(); // Create folder if not exists
        }

        String filePath = exportDirectory + File.separator + tableName + ".csv";

        try (CSVPrinter printer = new CSVPrinter(new FileWriter(filePath), CSVFormat.DEFAULT)) {
            List<String> headers = new ArrayList<>(rows.get(0).keySet());
            printer.printRecord(headers);

            for (Map<String, Object> row : rows) {
                List<String> values = headers.stream()
                        .map(h -> String.valueOf(row.get(h)))
                        .collect(Collectors.toList());
                printer.printRecord(values);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to export CSV file", e);
        }

        return "CSV file saved successfully at: " + filePath;
    }
}
