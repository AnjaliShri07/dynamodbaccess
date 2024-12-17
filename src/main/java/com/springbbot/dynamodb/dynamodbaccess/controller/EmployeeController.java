package com.springbbot.dynamodb.dynamodbaccess.controller;

import com.springbbot.dynamodb.dynamodbaccess.service.EmployeeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dynamodb")
public class EmployeeController {

    @Autowired
    private EmployeeService employeeService;

    @PostMapping("/createTable/{}")
    public String createTable(@PathVariable("tableName") String tableName) {
        employeeService.createTable(tableName);
        return "Table created: "+tableName;
    }

    @PostMapping("/insertItem")
    public String insertItem(@RequestParam String id,
                             @RequestParam String value) {
        employeeService.insertItem(id, value);
        return "Item inserted: " + id;
    }

    @GetMapping("/scan-table")
    public List<Map<String, String>> scanTable() {
        // Scan the table and fetch all items
        return employeeService.scanTable();
        //return "Scan complete. Check logs for results.";
    }

    @GetMapping("/findById/{id}")
    public Map<String, String> findById(@PathVariable("id") String id) {
        // Call the service to retrieve the item by ID
        Map<String, String> item = employeeService.findById(id);

        if (null != item) {
            return item; // Return the item as JSON if found
        } else {
            return Map.of("message", "Item not found"); // Return a message if not found
        }
    }

    // Update Item
    @PutMapping("/update/{id}/{value}")
    public String updateItem(@PathVariable String id, @PathVariable String value) {
        boolean isUpdated = employeeService.updateItem(id, value);
        if (isUpdated) {
            return "Item with ID " + id + " updated successfully.";
        } else {
            return "Failed to update item with ID " + id;
        }
    }


    @DeleteMapping("/delete/{id}")
    public String deleteById(@PathVariable("id") String id) {
        boolean isDeleted = employeeService.deleteById(id);
        if (isDeleted) {
            return "Item with ID " + id + " deleted successfully.";
        } else {
            return "Failed to delete item with ID " + id;
        }
    }

    @DeleteMapping("/table/{tableName}")
    public ResponseEntity<String> deleteTable(@PathVariable String tableName) {
        String result = employeeService.deleteTable(tableName);
        return ResponseEntity.ok(result);
    }

}
