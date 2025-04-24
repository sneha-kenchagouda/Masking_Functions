package com.est.masking.controller;

import com.est.masking.service.CsvExportService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class CsvExportController {

    @Autowired
    private CsvExportService csvExportService;

    @PostMapping("/export")
    public ResponseEntity<String> exportCsv(@RequestBody Map<String, Object> request) {
        String tableName = (String) request.get("tableName");

        long startTime = System.nanoTime();

        // If masking config is provided
        if (request.containsKey("columns")) {
            @SuppressWarnings("unchecked")
            Map<String, String> columnMap = (Map<String, String>) request.get("columns");
            String message = csvExportService.exportMaskedCsv(tableName, columnMap);
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            return ResponseEntity.ok(message + " | Masked export in " + duration + " ms");
        } else {
            String message = csvExportService.exportTableToCsv(tableName);
            long duration = (System.nanoTime() - startTime) / 1_000_000;
            return ResponseEntity.ok(message + " | Raw export in " + duration + " ms");
        }
    }
}
