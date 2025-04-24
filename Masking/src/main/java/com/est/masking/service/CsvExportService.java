package com.est.masking.service;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Service;

import java.io.*;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.*;

@Service
public class CsvExportService {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${csv.export.directory:C:/csv-files}")
    private String exportDirectory;

    public String exportTableToCsv(String tableName) {
        String filePath = exportDirectory + File.separator + tableName + ".csv";
        File dir = new File(exportDirectory);
        if (!dir.exists()) dir.mkdirs();

        String sql = "SELECT * FROM " + tableName;

        try (
                FileWriter fw = new FileWriter(filePath);
                BufferedWriter bw = new BufferedWriter(fw, 8192);
                CSVPrinter printer = new CSVPrinter(bw, CSVFormat.DEFAULT)
        ) {
            jdbcTemplate.query(sql, (ResultSetExtractor<Void>) rs -> {
                try {
                    writeResultSetToCsv(rs, printer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to write CSV file", e);
        }
        return "CSV file saved successfully at: " + filePath;
    }

    public String exportMaskedCsv(String tableName, Map<String, String> columnMap) {
        String filePath = exportDirectory + File.separator + tableName + "_masked.csv";
        File dir = new File(exportDirectory);
        if (!dir.exists()) dir.mkdirs();

        // Build SELECT clause
        StringBuilder sql = new StringBuilder("SELECT ");
        List<String> aliases = new ArrayList<>();

        for (Map.Entry<String, String> entry : columnMap.entrySet()) {
            String col = entry.getKey();
            String val = entry.getValue();

            if (val.trim().equalsIgnoreCase(col)) {
                sql.append(col).append(", ");
                aliases.add(col);
            } else {
                sql.append(val).append(" AS ").append(col).append(", ");
                aliases.add(col);
            }
        }

        // Remove last comma
        sql.setLength(sql.length() - 2);
        sql.append(" FROM ").append(tableName);

        try (
                FileWriter fw = new FileWriter(filePath);
                BufferedWriter bw = new BufferedWriter(fw, 8192);
                CSVPrinter printer = new CSVPrinter(bw, CSVFormat.DEFAULT)
        ) {
            jdbcTemplate.query(sql.toString(), (ResultSetExtractor<Void>) rs -> {
                try {
                    writeResultSetToCsv(rs, printer);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return null;
            });
        } catch (IOException e) {
            throw new RuntimeException("Failed to write masked CSV file", e);
        }

        return "Masked CSV file saved successfully at: " + filePath;
    }

    private void writeResultSetToCsv(ResultSet rs, CSVPrinter printer) throws IOException {
        try {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            List<String> headers = new ArrayList<>(columnCount);
            for (int i = 1; i <= columnCount; i++) {
                headers.add(metaData.getColumnName(i));
            }
            printer.printRecord(headers);

            while (rs.next()) {
                List<String> row = new ArrayList<>(columnCount);
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    row.add(value != null ? value.toString() : "");
                }
                printer.printRecord(row);
            }
        } catch (Exception e) {
            throw new IOException("CSV writing error", e);
        }
    }
}
