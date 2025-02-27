#ifndef LOGGER_H
#define LOGGER_H

#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <mutex>
#include <chrono>
#include <ctime>
#include <sys/time.h>
#include <iomanip>

enum class LogLevel {
    DEBUG,
    INFO,
    WARNING,
    ERROR,
    CRITICAL
};

class Logger {
public:
    // Singleton instance accessor
    static Logger& getInstance() {
        static Logger instance;
        return instance;
    }

    // Initialize the logger with output file
    void init(const std::string& logFilePath, LogLevel minLevel = LogLevel::INFO) {
        std::lock_guard<std::mutex> lock(mutex_);
        logFile_.open(logFilePath, std::ios::app);
        if (!logFile_.is_open()) {
            std::cerr << "Failed to open log file: " << logFilePath << std::endl;
        }
        minLevel_ = minLevel;
        initialized_ = true;
    }

    // Log a message with a specific level
    void log(LogLevel level, const std::string& message, const std::string& component = "") {
        if (level < minLevel_) return;
        
        std::lock_guard<std::mutex> lock(mutex_);
        std::string timestamp = getCurrentTimestamp();
        std::string levelStr = logLevelToString(level);
        std::string formattedMessage = formatMessage(timestamp, levelStr, message, component);
        
        // Output to console
        std::cout << formattedMessage << std::endl;
        
        // Output to file if initialized
        if (initialized_ && logFile_.is_open()) {
            logFile_ << formattedMessage << std::endl;
            logFile_.flush();
        }
    }

    // Convenience methods for different log levels
    void debug(const std::string& message, const std::string& component = "") {
        log(LogLevel::DEBUG, message, component);
    }
    
    void info(const std::string& message, const std::string& component = "") {
        log(LogLevel::INFO, message, component);
    }
    
    void warning(const std::string& message, const std::string& component = "") {
        log(LogLevel::WARNING, message, component);
    }
    
    void error(const std::string& message, const std::string& component = "") {
        log(LogLevel::ERROR, message, component);
    }
    
    void critical(const std::string& message, const std::string& component = "") {
        log(LogLevel::CRITICAL, message, component);
    }

    // Clean up
    ~Logger() {
        if (logFile_.is_open()) {
            logFile_.close();
        }
    }

private:
    // Private constructor to enforce singleton pattern
    Logger() : initialized_(false), minLevel_(LogLevel::INFO) {}
    
    // Prevent copying and assignment
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    // Get current timestamp with milliseconds
    std::string getCurrentTimestamp() {
        struct timeval tv;
        gettimeofday(&tv, nullptr);

        struct tm *tm_info = localtime(&tv.tv_sec);
        char buffer[64];
        strftime(buffer, sizeof(buffer), "%Y/%m/%d %H:%M:%S", tm_info);

        snprintf(buffer + strlen(buffer), sizeof(buffer) - strlen(buffer), ".%03ld", tv.tv_usec / 1000);
        return std::string(buffer);
    }

    // Convert LogLevel to string representation
    std::string logLevelToString(LogLevel level) {
        switch (level) {
            case LogLevel::DEBUG:    return "DEBUG";
            case LogLevel::INFO:     return "INFO";
            case LogLevel::WARNING:  return "WARNING";
            case LogLevel::ERROR:    return "ERROR";
            case LogLevel::CRITICAL: return "CRITICAL";
            default:                 return "UNKNOWN";
        }
    }

    // Format the log message
    std::string formatMessage(const std::string& timestamp, const std::string& level, 
                             const std::string& message, const std::string& component) {
        std::stringstream ss;
        ss << "[" << timestamp << "] ";
        ss << "[" << std::left << std::setw(8) << level << "] ";
        
        if (!component.empty()) {
            ss << "[" << component << "] ";
        }
        
        ss << message;
        return ss.str();
    }

    std::ofstream logFile_;
    std::mutex mutex_;
    bool initialized_;
    LogLevel minLevel_;
};

// Macro for easy access to logger instance
#define LOG Logger::getInstance()

#endif // LOGGER_H