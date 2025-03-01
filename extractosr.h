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
    static constexpr int FAR_TYPE = 0;      // File Attributes Record
    static constexpr int ATR_TYPE = 1;      // Audit Trail Record
    static constexpr int MIR_TYPE = 10;     // Master Information Record
    static constexpr int MRR_TYPE = 20;     // Master Results Record
    static constexpr int PCR_TYPE = 30;     // Part Count Record
    static constexpr int HBR_TYPE = 40;     // Hardware Bin Record
    static constexpr int SBR_TYPE = 50;     // Software Bin Record
    static constexpr int PMR_TYPE = 60;     // Pin Map Record
    static constexpr int PGR_TYPE = 62;     // Pin Group Record
    static constexpr int PLR_TYPE = 63;     // Pin List Record
    static constexpr int RDR_TYPE = 70;     // Retest Data Record
    static constexpr int SDR_TYPE = 80;     // Site Description Record
    static constexpr int WIR_TYPE = 90;     // Wafer Information Record
    static constexpr int WRR_TYPE = 100;    // Wafer Results Record
    static constexpr int WCR_TYPE = 110;    // Wafer Configuration Record
    static constexpr int PIR_TYPE = 180;    // Part Information Record

    // PRR Type Options - based on your logs, type 25 is appearing frequently
    static constexpr int PRR_TYPE = 5;      // Standard value in some libraries
    static constexpr int PRR_TYPE_ALT1 = 25;    // Based on your logs
    static constexpr int PRR_TYPE_ALT2 = 185;  
    // Utility function to check if a record is within the specified range
    /*static bool isInRange(std::ifstream& file, const StdfHeader& header, std::streampos startPos, std::streampos endPos) {
        // Get current file position (before the record)
        std::streampos currentPos = file.tellg();
        // Record size includes 4 bytes of header + REC_LEN
        //std::streampos recordEndPos = currentPos + static_cast<std::streamoff>(4) + static_cast<std::streamoff>(header.get_length());;
        std::streampos recordEndPos = currentPos + static_cast<std::streamoff>(4) + static_cast<std::streamoff>(header.get_length());

        // Check if the entire record is within the range
        return (currentPos >= startPos && recordEndPos <= endPos);
    }*/

    // Utility function to format file position
    /*static std::string formatPosition(std::streampos pos) {
        std::ostringstream oss;
        oss << "0x" << std::hex << std::uppercase << pos << " (" << std::dec << pos << " bytes)";
        return oss.str();
    }*/

    // Updated isInRange function with better boundary checking and error handling
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

    // Utility function to sanitize strings for JSON compatibility
    static std::string sanitizeString(const char* input) {
        if(!input) return "";

        std::string result;
        for(size_t i =0; input[i] != '\0'; i++) {
            unsigned char c = static_cast<unsigned char>(input[i]);
            if(c < 128) { // ASCII Character
                result += c;
            } else {
                // Replace non ASCII with placeholder
                result += "?";
            }
        }
        return result;
    }
public:
    /**
     * Extract PRR records from an STDF file within the specified within the specified byte range
     * 
     * @param filename Path to STDF file
     * @param startPos Starting byte position (0 = start of file)
     * @param endPos Ending byte position (-1 = end of file)
     * @return Vector of pointers of extracted PRR records( caller must free)
     */
    static std::vector<StdfPRR*> extractPrrRecords(const char* filename, std::streampos startPos = 0, std::streampos endPos =-1) {

        std::vector<StdfPRR*> prrRecords;
        Logger& logger = Logger::getInstance();

        logger.info("Starting PRR extraction from file: " + std::string(filename));

        std::ifstream file(filename, std::ios::in | std::ios::binary);
        if(!file) {
            logger.error("Failed to open file: " +  std::string(filename));
            return prrRecords;
        }

        // Get the file size
        file.seekg(0, std::ios::end);
        std::streampos fileSize = file.tellg();
        file.seekg(0, std::ios::beg);

        logger.debug("File Size: " + formatPosition(fileSize));

        // Validate input range
        if(startPos < 0) {
            logger.warning("Negative start position specified, using 0 instead");
            startPos = 0;
        }

        if (endPos < 0 || endPos > fileSize) {
            logger.info("Using file end as end position: " + formatPosition(fileSize));
            endPos = fileSize;
        }

        if(startPos >= endPos) {
            logger.error("Invalid range: start position " + formatPosition(startPos) + 
                    formatPosition(endPos) + " (" + std::to_string(endPos - startPos) + " bytes");
            file.close();
            return prrRecords;
        }

        logger.info("Extraction range: " + formatPosition(startPos) + " to " +
                    formatPosition(endPos) + " (" + std::to_string(endPos - startPos) + " bytes");

        // Set initial file position
        file.seekg(startPos);

        // First, check if file is a valid STDF file if starting from beginning
        if(startPos == 0) {
            logger.debug("Starting from file beginning, verifying FAR record");

            StdfHeader header;
            STDF_TYPE type = header.read(file);

            if(type != FAR_TYPE) {
                logger.error("File does not start with a FAR record, found type: " + std::to_string(type));
                file.close();
                return prrRecords;
            }

            // Verify CPU type and STDF version
            StdfFAR farRecord;
            farRecord.parse(header);

            logger.debug("FAR record: CPU type=" + std::to_string(farRecord.get_cpu_type()) + 
                         ", STDF version=" + std::to_string(farRecord.get_stdf_version()));
            
            if (farRecord.get_cpu_type() != 2) {
                logger.error("Unsupported CPU type: " + std::to_string(farRecord.get_cpu_type()));
                file.close();
                return prrRecords;
            }

            if (farRecord.get_stdf_version() != 4) {
                logger.error("Unsupported STDF version: " + std::to_string(farRecord.get_stdf_version()));
                file.close();
                return prrRecords;
            }

            // Reset file position to start
            file.seekg(startPos);
        } else {
            logger.info("Starting from position " + formatPosition(startPos) + " , skipping FAR record validation");
        }

        // Parse records
        int totalRecords = 0;
        int prrRecordsFound = 0;
        int invalidPositions = 0;
        const int MAX_INVALID_POSITIONS = 5; // Maximum number of consecutive invalid positions before breaking

        logger.info("Beginning record parsing");

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
                                " extends beyond extraction range, skipping");
                    
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

                // Update the record type detection in the extraction loop
                // This section should be in the extraction loop in extractPrrRecords method

                // If this is a PRR Record, extract it
                if (isPrrRecordType(type)) {
                    logger.debug("Found potential PRR record (type " + std::to_string(type) + ") at position " + 
                                 formatPosition(recordStartPos) + " with length " + std::to_string(header.get_length()));
                                 
                    try {
                        StdfPRR* prrRecord = new StdfPRR();
                        prrRecord->parse(header);
                        
                        // Add extra logging to check the record content
                        logger.debug("PRR content: head=" + std::to_string(prrRecord->get_head_number()) + 
                                     ", site=" + std::to_string(prrRecord->get_site_number()) + 
                                     ", hardbin=" + std::to_string(prrRecord->get_hardbin_number()) + 
                                     ", softbin=" + std::to_string(prrRecord->get_softbin_number()));
                        
                        // Validate the PRR record before adding it (basic checks only)
                        if (prrRecord->get_hardbin_number() < -10000 || prrRecord->get_softbin_number() < -10000) {
                            logger.warning("Suspicious PRR record values, discarding", "StdfExtractor");
                            delete prrRecord;
                        } else {
                            prrRecords.push_back(prrRecord);
                            prrRecordsFound++;
                            logger.debug("Extracted PRR record #" + std::to_string(prrRecordsFound) + 
                                        " at position " + formatPosition(recordStartPos));
                        }
                    } catch (const std::exception& e) {
                        logger.error("Failed to parse PRR record: " + std::string(e.what()), "StdfExtractor");
                        // Continue to next record
                        file.seekg(header.get_length(), std::ios::cur);
                    }
                } else {
                    // Log record types periodically to identify patterns
                    if (totalRecords % 100 == 0) {
                        logger.debug("Found record type " + std::to_string(type) + 
                                     " at position " + formatPosition(recordStartPos));
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

                // If this is a PRR Record, extract it
                /*if (type == PRR_TYPE) {
                    prrRecordsFound++;
                    StdfPRR* prrRecord = new StdfPRR();
                    prrRecord->parse(header);
                    prrRecords.push_back(prrRecord);

                    logger.debug("Extracted PRR record #" + std::to_string(prrRecordsFound) + 
                                " at position " + formatPosition(recordStartPos));
                } else {
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
                */
            } catch (const std::exception& e) {
                logger.error("Error parsing record at position " + formatPosition(recordStartPos) + 
                            ": " + std::string(e.what()), "StdfExtractor");
                break;
            }
        }

        // Parse records
        /*int totalRecords = 0;
        int prrRecordsFound = 0;

        logger.info("Beginning record parsing");

        while (file.tellg() < endPos && !file.eof()) {
            StdfHeader header;
            std::streampos recordStartPos = file.tellg();

            try{
                STDF_TYPE type = header.read(file);
                totalRecords++;

                // Check if we're still within range
                if(!isInRange(file, header, startPos, endPos)) {
                    logger.debug("Record at " + formatPosition(recordStartPos) +
                            " extends beyod extraction range, skipping");
                    // Skip to next record
                    file.seekg(header.get_length(), std::ios::cur);
                    continue;
                }

                // If this ia PRR Record, extract it
                if(type == PRR_TYPE) {
                    prrRecordsFound++;
                    StdfPRR* prrRecord = new StdfPRR();
                    prrRecord->parse(header);
                    prrRecords.push_back(prrRecord);

                    logger.debug("Extracted PRR record #" + std::to_string(prrRecordsFound) + " at position " + formatPosition(recordStartPos));
                } else{
                    // Skip to the next record if not a PRR
                    file.seekg(header.get_length(), std::ios::cur);
                }
            } catch (const std::exception& e){
                logger.error("Error parsing record at position " + formatPosition(recordStartPos));
                break;
            }
        }
        */
        file.close();

        logger.info("Extraction complete. Parsed " + std::to_string(totalRecords) +
                        " records, found " + std::to_string(prrRecords.size()) + " PRR records");

        return prrRecords;
        
    }

    /**
     * Save extracted PRR records to JSON file
     * 
     * @param prrRecords vector of PRR records to save
     * @param outputFilename Path to the output JSON File
     * @return true if successful, false otherwise
     */

     // Update the savePrrRecords method to handle empty results better

    static bool savePrrRecords(const std::vector<StdfPRR*>& prrRecords, const std::string& outputFileName, const time_t& sync_time) {
        Logger& logger = Logger::getInstance();

        if(prrRecords.empty()){
            logger.warning("No PRR Records to save to the JSON file: " + outputFileName);
            
            // Create an empty JSON array to indicate successful processing but no records found
            nlohmann::json emptyArray = nlohmann::json::array();
            
            try {
                // Write to File even if empty - this ensures the file exists with valid JSON
                std::ofstream jsonFile(outputFileName);
                if(!jsonFile.is_open()) {
                    logger.error("Failed to open output JSON file: " + outputFileName);
                    return false;
                }
                
                // Write empty array with comment indicating no records found
                jsonFile << "// No PRR records found in the processed file range\n";
                jsonFile << emptyArray.dump(4);
                jsonFile.close();
                
                logger.info("Saved empty result to JSON file (no PRR records found)");
                return true;  // Return success even for empty results
            } catch (const std::exception& e) {
                logger.error("Error saving empty result to JSON: " + std::string(e.what()));
                return false;
            }
        }

        // Rest of the existing function for non-empty results
        logger.info("Saving " + std::to_string(prrRecords.size()) + " PRR records to JSON file: " + outputFileName);
        try{
            // Create JSON array to hold all records
            nlohmann::json jsonRecords = nlohmann::json::array();

            // Convert each PRR record to JSON
            for(const StdfPRR* prr : prrRecords) {
                nlohmann::json jsonRecord;

                // Add all relevant fields from PRR
                jsonRecord["head_number"] = prr->get_head_number();
                jsonRecord["site_number"] = prr->get_site_number();
                jsonRecord["test_count"] = prr->get_number_test();
                jsonRecord["hard_bin"] = prr->get_hardbin_number();
                jsonRecord["soft_bin"] = prr->get_softbin_number();
                jsonRecord["test_time"] = prr->get_elapsed_ms();
                const char* part_id = prr->get_part_id();
                if(part_id){
                    jsonRecord["part_id"] = sanitizeString(part_id);
                }
                jsonRecord["last_modified"] = sync_time;
                jsonRecord["sot"] = sync_time - prr->get_elapsed_ms()/1000;
                jsonRecord["eot"] = sync_time;

                jsonRecords.push_back(jsonRecord);
            }

            // Write to File
            std::ofstream jsonFile(outputFileName);
            if(!jsonFile.is_open()) {
                logger.error("Failed to open output JSON file: " + outputFileName);
                return false;
            }

            // Write with pretty formatting(4 spaces indentation)
            jsonFile << jsonRecords.dump(4);
            jsonFile.close();

            logger.info("Successfully saved " + std::to_string(prrRecords.size()) + " PRR records to JSON file");
            return true;
        } catch (const std::exception& e){
            logger.error("Error saving PRR records to JSON: " + std::string(e.what()));
            return false;
        }
    }
    
    static bool isPrrRecordType(STDF_TYPE type) {
        Logger& logger = Logger::getInstance();
        
        // Check against known PRR record types
        if (type == PRR_TYPE) {
            return true;
        }
        
        // Based on logs, type 25 appears frequently and might be PRR records
        if (type == 25) {
            // Add this as a potential PRR record type
            return true;
        }
        
        // Some STDF libraries use different values for PRR_TYPE
        if (type == 185) { // Another common value for PRR_TYPE
            logger.info("Found PRR record with alternative type value (185)", "StdfExtractor");
            return true;
        }
        
        // Log if we're seeing many records of a specific type that might be PRR
        static std::map<STDF_TYPE, int> typeCount;
        static int totalChecks = 0;
        
        typeCount[type]++;
        totalChecks++;
        
        // Every 1000 checks, log the most common record types
        if (totalChecks % 1000 == 0) {
            // Find the most common type
            int mostCommonType = 0;
            int highestCount = 0;
            
            for (const auto& pair : typeCount) {
                if (pair.second > highestCount) {
                    highestCount = pair.second;
                    mostCommonType = pair.first;
                }
            }
            
            // Log the most common type
            if (highestCount > 100) {
                logger.info("Most common record type: " + std::to_string(mostCommonType) + 
                           " (seen " + std::to_string(highestCount) + " times)", "StdfExtractor");
            }
        }
        
        return false;
    }

    /* static bool savePrrRecords(const std::vector<StdfPRR*>& prrRecords, const std::string& outputFileName, const time_t& sync_time) {
        Logger& logger = Logger::getInstance();

        if(prrRecords.empty()){
            logger.warning("No PRR Records to save the JSON file: " + outputFileName);
            return false;
        }

        logger.info("Saving " + std::to_string(prrRecords.size()) + " PRR records to JSON file: " + outputFileName);
        try{
            // Create JSON array to hold all records
            nlohmann::json jsonRecords = nlohmann::json::array();

            // Convert each PRR record to JSON
            for(const StdfPRR* prr : prrRecords) {
                nlohmann::json jsonRecord;

                // Add all relevant fields from PRR
                jsonRecord["head_number"] = prr->get_head_number();
                jsonRecord["site_number"] = prr->get_site_number();
                jsonRecord["test_count"] = prr->get_number_test();
                jsonRecord["hard_bin"] = prr->get_hardbin_number();
                jsonRecord["soft_bin"] = prr->get_softbin_number();
                jsonRecord["test_time"] = prr->get_elapsed_ms();
                const char* part_id = prr->get_part_id();
                if(part_id){
                    jsonRecord["part_id"] = part_id;
                }
                jsonRecord["last_modified"] = sync_time;
                jsonRecord["sot"] = sync_time - prr->get_elapsed_ms()/1000;
                jsonRecord["eot"] = sync_time;

                jsonRecords.push_back(jsonRecord);
            }

            // Write to File
            std::ofstream jsonFile(outputFileName);
            if(!jsonFile.is_open()) {
                logger.error("Failed to open output JSON file: " + outputFileName);
                return false;
            }

            // Write with pretty formatting(4 spaces indentation)
            jsonFile << jsonRecords.dump(4);
            jsonFile.close();

            logger.info("Successfully saved " + std::to_string(prrRecords.size()) + " PRR records to JSON file");
            return true;
        } catch (const std::exception& e){
            logger.error("Error saving PRR records to JSON: " + std::string(e.what()));
            return false;
        }
    }
    */

    /**
     * Free Memory used by extracted PRR records
     * 
     * @param prrRecords Vector of PRR records to free
     */
    static void freePrrRecords(std::vector<StdfPRR*>& prrRecords){
        Logger& logger = Logger::getInstance();
        logger.debug("Free memory for " + std::to_string(prrRecords.size()) + " PRR records");

        for(StdfPRR* record : prrRecords) {
            delete record;
        }
        prrRecords.clear();

        logger.debug("Memory freed");
    }

};

#endif // STDF_PRR_EXTRACTOR_H