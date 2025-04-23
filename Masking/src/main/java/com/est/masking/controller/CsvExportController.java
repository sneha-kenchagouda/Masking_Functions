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
    public ResponseEntity<String> exportCsv(@RequestBody Map<String, String> request) {
        String tableName = request.get("tableName");
        String message = csvExportService.exportTableToCsv(tableName);
        return ResponseEntity.ok(message);
    }
}
