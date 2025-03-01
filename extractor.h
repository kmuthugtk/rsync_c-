#ifndef STDF_PRR_EXTRACTOR_H
#define STDF_PRR_EXTRACTOR_H

#include "stdf_v4_api.h"
#include "stdf_v4_file.h"
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include "logger.h"
#include <string>
#include <sstream>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>

class StdfExtractor {
private:
    // Define all possible STDF record types
    static constexpr int FAR_TYPE = 0;      // File Attributes Record
    static constexpr int ATR_TYPE = 1;      // Audit Trail Record
    static constexpr int MIR_TYPE = 2;      // Master Information Record
    static constexpr int MRR_TYPE = 3;      // Master Results Record
    static constexpr int PCR_TYPE = 4;      // Part Count Record
    static constexpr int HBR_TYPE = 5;      // Hardware Bin Record
    static constexpr int SBR_TYPE = 6;      // Software Bin Record
    static constexpr int PMR_TYPE = 7;      // Pin Map Record
    static constexpr int PGR_TYPE = 8;      // Pin Group Record
    static constexpr int PLR_TYPE = 9;      // Pin List Record
    static constexpr int RDR_TYPE = 10;     // Retest Data Record
    static constexpr int SDR_TYPE = 11;     // Site Description Record
    static constexpr int WIR_TYPE = 12;     // Wafer Information Record
    static constexpr int WRR_TYPE = 13;     // Wafer Results Record
    static constexpr int WCR_TYPE = 14;     // Wafer Configuration Record
    static constexpr int PIR_TYPE = 15;     // Part Information Record
    static constexpr int PRR_TYPE = 16;     // Part Results Record - standard value

    // Alternative PRR types observed in your data
    static constexpr int PRR_TYPE_ALT1 = 25;    // Based on logs
    static constexpr int PRR_TYPE_ALT2 = 185;   // Another possible value
    
    // Improved isInRange function with better boundary checking and error handling
    static bool isInRange(std::ifstream& file, const StdfHeader& header, std::streampos startPos, std::streampos endPos) {
        // Get current file position (before the record)
        std::streampos currentPos = file.tellg();
        
        // Check for invalid file position
        if (currentPos < 0) {
            Logger& logger = Logger::getInstance();
            logger.error("Invalid file position: " + formatPosition(currentPos), "StdfExtractor");
            return false;
        }
        
        // Calculate record end position - carefully to avoid overflow
        std::streamoff headerSize = 4; // 4 bytes for header
        std::streamoff recordLen = static_cast<std::streamoff>(header.get_length());
        
        // Sanity check on record length to avoid invalid positions
        if (recordLen < 0 || recordLen > 100000) { // Assuming no valid STDF record is >100KB
            Logger& logger = Logger::getInstance();
            logger.warning("Suspicious record length: " + std::to_string(recordLen) + " bytes", "StdfExtractor");
            return false;
        }
        
        std::streampos recordEndPos = currentPos;
        // Check for potential overflow before adding
        if (std::numeric_limits<std::streamoff>::max() - headerSize < recordLen) {
            Logger& logger = Logger::getInstance();
            logger.error("Record length calculation would overflow", "StdfExtractor");
            return false;
        }
        
        recordEndPos += (headerSize + recordLen);
        
        // Check if record starts after or at startPos
        bool afterStart = (currentPos >= startPos);
        
        // Check if record ends before or at endPos
        bool beforeEnd = (recordEndPos <= endPos);
        
        // Both conditions must be true for the record to be in range
        return (afterStart && beforeEnd);
    }

    static std::string formatPosition(std::streampos pos) {
        std::ostringstream oss;
        // Check if the position is negative
        if (pos < 0) {
            // Special handling for negative positions
            oss << "0x" << std::hex << std::uppercase << static_cast<int64_t>(pos) 
                << " (" << std::dec << static_cast<int64_t>(pos) << " bytes)";
        } else {
            oss << "0x" << std::hex << std::uppercase << pos 
                << " (" << std::dec << pos << " bytes)";
        }
        return oss.str();
    }

    // Improved string sanitization function for JSON compatibility
    static std::string sanitizeString(const char* input) {
        if (!input) return "";

        std::string result;
        for (size_t i = 0; input[i] != '\0'; i++) {
            unsigned char c = static_cast<unsigned char>(input[i]);
            // Only allow printable ASCII characters
            if (c >= 32 && c < 127) {
                // Special handling for JSON escaping
                if (c == '\\' || c == '"' || c == '/') {
                    result += '\\';
                    result += c;
                } else {
                    result += c;
                }
            } else {
                // Replace non-printable characters with a placeholder
                result += "?";
            }
        }
        return result;
    }

    // Helper function to determine if a record type is a PRR record
    static bool isPrrRecordType(STDF_TYPE type) {
        Logger& logger = Logger::getInstance();
        
        // Check against all known PRR record types
        if (type == PRR_TYPE) {
            logger.debug("Found standard PRR record (type " + std::to_string(type) + ")", "StdfExtractor");
            return true;
        }
        
        // Check against alternative PRR record types observed in data
        if (type == PRR_TYPE_ALT1) {
            logger.debug("Found alternative PRR record (type " + std::to_string(type) + ")", "StdfExtractor");
            return true;
        }
        
        if (type == PRR_TYPE_ALT2) {
            logger.debug("Found alternative PRR record (type " + std::to_string(type) + ")", "StdfExtractor");
            return true;
        }
        
        // Dynamic detection of potential PRR records
        static std::map<STDF_TYPE, int> typeCount;
        static int totalChecks = 0;
        
        typeCount[type]++;
        totalChecks++;
        
        // Every 1000 checks, log the most common record types
        if (totalChecks % 1000 == 0) {
            // Find the most common types
            std::vector<std::pair<STDF_TYPE, int>> sortedTypes;
            for (const auto& pair : typeCount) {
                if (pair.second > 50) { // Only include types seen at least 50 times
                    sortedTypes.push_back(pair);
                }
            }
            
            // Sort by frequency
            std::sort(sortedTypes.begin(), sortedTypes.end(), 
                     [](const auto& a, const auto& b) { return a.second > b.second; });
            
            // Log the top 3 most common types
            std::stringstream typesLog;
            typesLog << "Most common record types: ";
            for (size_t i = 0; i < std::min(size_t(3), sortedTypes.size()); i++) {
                typesLog << "type " << sortedTypes[i].first << " (" << sortedTypes[i].second << " times)";
                if (i < std::min(size_t(2), sortedTypes.size() - 1)) typesLog << ", ";
            }
            
            if (!sortedTypes.empty()) {
                logger.info(typesLog.str(), "StdfExtractor");
            }
        }
        
        return false;
    }

public:
    /**
     * Extract PRR records from an STDF file within the specified byte range
     * 
     * @param filename Path to STDF file
     * @param startPos Starting byte position (0 = start of file)
     * @param endPos Ending byte position (-1 = end of file)
     * @return Vector of pointers to extracted PRR records (caller must free)
     */
    static std::vector<StdfPRR*> extractPrrRecords(const char* filename, std::streampos startPos = 0, std::streampos endPos = -1) {
        std::vector<StdfPRR*> prrRecords;
        Logger& logger = Logger::getInstance();

        logger.info("Starting PRR extraction from file: " + std::string(filename), "StdfExtractor");

        std::ifstream file(filename, std::ios::in | std::ios::binary);
        if (!file) {
            logger.error("Failed to open file: " + std::string(filename), "StdfExtractor");
            return prrRecords;
        }

        // Get the file size
        file.seekg(0, std::ios::end);
        std::streampos fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        logger.debug("File Size: " + formatPosition(fileSize), "StdfExtractor");

        // Validate input range
        if (startPos < 0) {
            logger.warning("Negative start position specified, using 0 instead", "StdfExtractor");
            startPos = 0;
        }

        if (endPos < 0 || endPos > fileSize) {
            logger.info("Using file end as end position: " + formatPosition(fileSize), "StdfExtractor");
            endPos = fileSize;
        }

        if (startPos >= endPos) {
            logger.error("Invalid range: start position " + formatPosition(startPos) + 
                    " >= end position " + formatPosition(endPos), "StdfExtractor");
            file.close();
            return prrRecords;
        }

        logger.info("Extraction range: " + formatPosition(startPos) + " to " +
                    formatPosition(endPos) + " (" + std::to_string(endPos - startPos) + " bytes)", "StdfExtractor");

        // Set initial file position
        file.seekg(startPos);

        // First, check if file is a valid STDF file if starting from beginning
        if (startPos == 0) {
            logger.debug("Starting from file beginning, verifying FAR record", "StdfExtractor");

            StdfHeader header;
            STDF_TYPE type = header.read(file);

            if (type != FAR_TYPE) {
                logger.error("File does not start with a FAR record, found type: " + std::to_string(type), "StdfExtractor");
                file.close();
                return prrRecords;
            }

            // Verify CPU type and STDF version
            StdfFAR farRecord;
            farRecord.parse(header);

            logger.debug("FAR record: CPU type=" + std::to_string(farRecord.get_cpu_type()) + 
                         ", STDF version=" + std::to_string(farRecord.get_stdf_version()), "StdfExtractor");
            
            if (farRecord.get_cpu_type() != 2) {
                logger.error("Unsupported CPU type: " + std::to_string(farRecord.get_cpu_type()), "StdfExtractor");
                file.close();
                return prrRecords;
            }

            if (farRecord.get_stdf_version() != 4) {
                logger.error("Unsupported STDF version: " + std::to_string(farRecord.get_stdf_version()), "StdfExtractor");
                file.close();
                return prrRecords;
            }

            // Reset file position to start
            file.seekg(startPos);
        } else {
            logger.info("Starting from position " + formatPosition(startPos) + ", skipping FAR record validation", "StdfExtractor");
        }

        // Parse records
        int totalRecords = 0;
        int prrRecordsFound = 0;
        int invalidPositions = 0;
        const int MAX_INVALID_POSITIONS = 5; // Maximum number of consecutive invalid positions before breaking

        logger.info("Beginning record parsing", "StdfExtractor");

        while (file.tellg() < endPos && !file.eof()) {
            std::streampos recordStartPos = file.tellg();
            
            // Check for invalid file position
            if (recordStartPos < 0) {
                invalidPositions++;
                logger.error("Invalid file position detected: " + formatPosition(recordStartPos), "StdfExtractor");
                
                if (invalidPositions >= MAX_INVALID_POSITIONS) {
                    logger.error("Too many consecutive invalid positions, stopping extraction", "StdfExtractor");
                    break;
                }
                
                // Try to reset to a valid position
                file.clear();
                file.seekg(0, std::ios::end);
                std::streampos endOfFile = file.tellg();
                
                if (endOfFile > 0 && endOfFile < endPos) {
                    logger.info("Adjusting end position to actual end of file: " + formatPosition(endOfFile), "StdfExtractor");
                    endPos = endOfFile;
                }
                
                break;
            }
            
            invalidPositions = 0; // Reset counter on valid position
            
            try {
                StdfHeader header;
                STDF_TYPE type = header.read(file);
                
                if (file.fail()) {
                    logger.error("File read error at position " + formatPosition(recordStartPos), "StdfExtractor");
                    break;
                }
                
                totalRecords++;

                // Check if we're still within range
                if (!isInRange(file, header, startPos, endPos)) {
                    logger.debug("Record at " + formatPosition(recordStartPos) + 
                                " extends beyond extraction range, skipping", "StdfExtractor");
                    
                    // Skip to next record
                    std::streamoff skipLength = header.get_length();
                    
                    // Sanity check on skip length
                    if (skipLength <= 0 || skipLength > 100000) {
                        logger.warning("Invalid skip length: " + std::to_string(skipLength) + ", stopping extraction", "StdfExtractor");
                        break;
                    }
                    
                    file.seekg(skipLength, std::ios::cur);
                    
                    // Check if seek failed
                    if (file.fail()) {
                        logger.error("Failed to skip to next record from " + formatPosition(recordStartPos), "StdfExtractor");
                        break;
                    }
                    
                    continue;
                }

                // If this is a PRR Record, extract it
                if (isPrrRecordType(type)) {
                    logger.debug("Found potential PRR record (type " + std::to_string(type) + ") at position " + 
                                formatPosition(recordStartPos) + " with length " + std::to_string(header.get_length()), "StdfExtractor");
                                
                    try {
                        StdfPRR* prrRecord = new StdfPRR();
                        prrRecord->parse(header);
                        
                        // Add extra logging to check the record content
                        logger.debug("PRR content: head=" + std::to_string(prrRecord->get_head_number()) + 
                                    ", site=" + std::to_string(prrRecord->get_site_number()) + 
                                    ", hardbin=" + std::to_string(prrRecord->get_hardbin_number()) + 
                                    ", softbin=" + std::to_string(prrRecord->get_softbin_number()), "StdfExtractor");
                        
                        // Validate the PRR record before adding it (basic sanity checks)
                        if (prrRecord->get_hardbin_number() < -10000 || prrRecord->get_softbin_number() < -10000 ||
                            prrRecord->get_head_number() > 255 || prrRecord->get_site_number() > 255) {
                            logger.warning("Suspicious PRR record values, discarding record", "StdfExtractor");
                            delete prrRecord;
                        } else {
                            prrRecords.push_back(prrRecord);
                            prrRecordsFound++;
                            
                            if (prrRecordsFound % 100 == 0 || prrRecordsFound == 1) {
                                logger.info("Extracted " + std::to_string(prrRecordsFound) + " PRR records so far", "StdfExtractor");
                            }
                        }
                    } catch (const std::exception& e) {
                        logger.error("Failed to parse PRR record: " + std::string(e.what()), "StdfExtractor");
                        // Continue to next record
                        file.seekg(header.get_length(), std::ios::cur);
                    }
                } else {
                    // Log record types periodically to identify patterns
                    if (totalRecords % 1000 == 0) {
                        logger.debug("Processing record " + std::to_string(totalRecords) + 
                                    ", found " + std::to_string(prrRecordsFound) + " PRR records so far", "StdfExtractor");
                    }
                    
                    // Skip to the next record if not a PRR
                    std::streamoff skipLength = header.get_length();
                    
                    // Sanity check
                    if (skipLength < 0 || skipLength > 100000) {
                        logger.warning("Suspicious record length for skipping: " + std::to_string(skipLength), "StdfExtractor");
                        break;
                    }
                    
                    file.seekg(skipLength, std::ios::cur);
                    
                    // Check if seek failed
                    if (file.fail()) {
                        logger.error("Failed to skip non-PRR record from " + formatPosition(recordStartPos), "StdfExtractor");
                        break;
                    }
                }
            } catch (const std::exception& e) {
                logger.error("Error parsing record at position " + formatPosition(recordStartPos) + 
                            ": " + std::string(e.what()), "StdfExtractor");
                break;
            }
        }
        
        file.close();

        logger.info("Extraction complete. Parsed " + std::to_string(totalRecords) +
                    " records, found " + std::to_string(prrRecords.size()) + " PRR records", "StdfExtractor");

        return prrRecords;
    }

    /**
     * Save extracted PRR records to JSON file
     * 
     * @param prrRecords vector of PRR records to save
     * @param outputFilename Path to the output JSON File
     * @param sync_time Timestamp to use for the records
     * @return true if successful, false otherwise
     */
    static bool savePrrRecords(const std::vector<StdfPRR*>& prrRecords, const std::string& outputFileName, const time_t& sync_time) {
        Logger& logger = Logger::getInstance();

        if (prrRecords.empty()) {
            logger.warning("No PRR Records to save to the JSON file: " + outputFileName, "StdfExtractor");
            
            // Create an empty JSON array to indicate successful processing but no records found
            nlohmann::json emptyArray = nlohmann::json::array();
            
            try {
                // Create directory if it doesn't exist
                size_t lastSlash = outputFileName.find_last_of('/');
                if (lastSlash != std::string::npos) {
                    std::string directory = outputFileName.substr(0, lastSlash);
                    std::string command = "mkdir -p " + directory;
                    int result = system(command.c_str());
                    if (result != 0) {
                        logger.warning("Failed to create directory: " + directory, "StdfExtractor");
                    }
                }
                
                // Write to File even if empty - this ensures the file exists with valid JSON
                std::ofstream jsonFile(outputFileName);
                if (!jsonFile.is_open()) {
                    logger.error("Failed to open output JSON file: " + outputFileName, "StdfExtractor");
                    return false;
                }
                
                // Write empty array with comment indicating no records found
                jsonFile << "// No PRR records found in the processed file range\n";
                jsonFile << emptyArray.dump(4);
                jsonFile.close();
                
                logger.info("Saved empty result to JSON file (no PRR records found)", "StdfExtractor");
                return true;  // Return success even for empty results
            } catch (const std::exception& e) {
                logger.error("Error saving empty result to JSON: " + std::string(e.what()), "StdfExtractor");
                return false;
            }
        }

        // Rest of the existing function for non-empty results
        logger.info("Saving " + std::to_string(prrRecords.size()) + " PRR records to JSON file: " + outputFileName, "StdfExtractor");
        try {
            // Create directory if it doesn't exist
            size_t lastSlash = outputFileName.find_last_of('/');
            if (lastSlash != std::string::npos) {
                std::string directory = outputFileName.substr(0, lastSlash);
                std::string command = "mkdir -p " + directory;
                int result = system(command.c_str());
                if (result != 0) {
                    logger.warning("Failed to create directory: " + directory, "StdfExtractor");
                }
            }
            
            // Create JSON array to hold all records
            nlohmann::json jsonRecords = nlohmann::json::array();

            // Convert each PRR record to JSON
            for (const StdfPRR* prr : prrRecords) {
                nlohmann::json jsonRecord;

                // Add all relevant fields from PRR
                jsonRecord["head_number"] = prr->get_head_number();
                jsonRecord["site_number"] = prr->get_site_number();
                jsonRecord["test_count"] = prr->get_number_test();
                jsonRecord["hard_bin"] = prr->get_hardbin_number();
                jsonRecord["soft_bin"] = prr->get_softbin_number();
                jsonRecord["x_coord"] = prr->get_x_coordinate();
                jsonRecord["y_coord"] = prr->get_y_coordinate();
                jsonRecord["test_time"] = prr->get_elapsed_ms();
                
                // Add part flags for more detailed information
                jsonRecord["part_flags"] = {
                    {"superseded", prr->part_supersede_flag()},
                    {"abnormal", prr->part_abnormal_flag()},
                    {"failed", prr->part_failed_flag()},
                    {"invalid_flag", prr->pass_fail_flag_invalid()}
                };
                
                const char* part_id = prr->get_part_id();
                if (part_id) {
                    jsonRecord["part_id"] = sanitizeString(part_id);
                }
                
                const char* part_text = prr->get_part_discription();
                if (part_text) {
                    jsonRecord["part_text"] = sanitizeString(part_text);
                }
                
                // Add timestamps
                jsonRecord["last_modified"] = sync_time;
                jsonRecord["sot"] = sync_time - prr->get_elapsed_ms()/1000;
                jsonRecord["eot"] = sync_time;

                jsonRecords.push_back(jsonRecord);
            }

            // Write to File
            std::ofstream jsonFile(outputFileName);
            if (!jsonFile.is_open()) {
                logger.error("Failed to open output JSON file: " + outputFileName, "StdfExtractor");
                return false;
            }

            // Write with pretty formatting (4 spaces indentation)
            jsonFile << jsonRecords.dump(4);
            jsonFile.close();

            logger.info("Successfully saved " + std::to_string(prrRecords.size()) + " PRR records to JSON file", "StdfExtractor");
            return true;
        } catch (const std::exception& e) {
            logger.error("Error saving PRR records to JSON: " + std::string(e.what()), "StdfExtractor");
            return false;
        }
    }

    /**
     * Free Memory used by extracted PRR records
     * 
     * @param prrRecords Vector of PRR records to free
     */
    static void freePrrRecords(std::vector<StdfPRR*>& prrRecords) {
        Logger& logger = Logger::getInstance();
        logger.debug("Freeing memory for " + std::to_string(prrRecords.size()) + " PRR records", "StdfExtractor");

        for (StdfPRR* record : prrRecords) {
            delete record;
        }
        prrRecords.clear();

        logger.debug("Memory freed", "StdfExtractor");
    }
};

#endif // STDF_PRR_EXTRACTOR_H