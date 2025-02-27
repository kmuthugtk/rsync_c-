#include <iostream>
#include <fstream>
#include <vector>
#include <regex>
#include <memory>
#include <chrono>
#include <thread>
#include <cstdio>
#include <ctime>
#include <sys/time.h>
#include <iomanip>
#include <rabbitmq-c/amqp.h>
#include <rabbitmq-c/tcp_socket.h>
#include <stdf_reader/stdf_v4_api.h>
#include <stdf_reader/stdf_v4_file.h>
#include "logger.h"

// RabbitMQ Server COnfiguration
const std::string RABBITMQ_HOST = "10.100.246.53";
const int RABBITMQ_PORT = 5672;
const std::string RABBITMQ_USER = "system";
const std::string RABBITMQ_PASSWORD = "system";
const std::string RABBITMQ_VHOST = "/";
const std::string QUEUE_NAME = "LPX-67";
const std::string EXCHANGE_NAME = "";
const std::string ROUTING_KEY = "LPX-67";
const int CHANNEL_ID = 1;

std::string getCurrentTimestamp() {
    struct timeval tv;
    gettimeofday(&tv, nullptr);

    struct tm *tm_info = localtime(&tv.tv_sec);
    char buffer[64];
    strftime(buffer, sizeof(buffer), "%Y/%m/%d %H:%M:%S", tm_info);

    snprintf(buffer + strlen(buffer), sizeof(buffer) - strlen(buffer), ".%03ld", tv.tv_usec / 1000);
    return std::string(buffer);
}

// Function to Handle Incoming messages
void consumeMessages() {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);

    if(!socket) {
        //std::cerr << getCurrentTimestamp() << "Failed to create TCP socket" << std::endl;
        LOG.error("Failed to create TCP socket", "RabbitMQ");
        return;
    }

    if(amqp_socket_open(socket, RABBITMQ_HOST.c_str(), RABBITMQ_PORT)) {
        std::cerr << getCurrentTimestamp() << "Failed to open TCP connection" << std::endl;
        LOG.error("Failed to open TCP connection", "RabbitMQ");
        return;
    }

    LOG.debug("TCP socket opened successfully", "RabbitMQ");

    amqp_rpc_reply_t login_reply = amqp_login(
        conn,
        RABBITMQ_VHOST.c_str(),
        0,
        131072,
        0,
        AMQP_SASL_METHOD_PLAIN,
        RABBITMQ_USER.c_str(),
        RABBITMQ_PASSWORD.c_str()
    );

    if(login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << "Failed to log in to RabbitMQ" << std::endl;
        LOG.error("Failed to log in to RabbitMQ", "RabbitMQ");
        return;
    }

    LOG.info("Successfully logged in to RabbitMQ server", "RabbitMQ");

    amqp_channel_open(conn, CHANNEL_ID);
    if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
        std::cerr << getCurrentTimestamp() << " Failed to open a channel" << std::endl;
        return;
    }

    // Set QoS - Prefetch count = 1
    amqp_basic_qos(
        conn,           // Connection
        CHANNEL_ID,     // Channel
        0,              // prefetch size (0 means "no specific limit")
        1,              // prefetch count (1 message at a time)
        0               // global (0 = per-consumer, 1 = per-channel)
    );
    if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << " Failed to set QoS" << std::endl;
        LOG.error("Failed to set QoS", "RabbitMQ");
        return;
    }

    amqp_queue_declare(
        conn,
        CHANNEL_ID,
        amqp_cstring_bytes(QUEUE_NAME.c_str()),
        0, // passive: 0 = create if not exists
        1, // durable: 1 = survive server restarts
        0, // exclusive: 0 = non-exclusive
        0, // auto-delete: 0 = do not auto-delete when not in use
        amqp_empty_table
    );

    if (amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << " Failed to declare queue" << std::endl;
        LOG.error("Failed to declare queue", "RabbitMQ");
        return;
    }
    LOG.info("Queue declared: " + QUEUE_NAME, "RabbitMQ");

    amqp_basic_consume(
        conn,
        CHANNEL_ID,
        amqp_cstring_bytes(QUEUE_NAME.c_str()),
        amqp_empty_bytes,
        0,
        1,
        0,
        amqp_empty_table
    );

    if(amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << " Failed to start consuming messages" << std::endl;
        LOG.error("Failed to start consuming messages", "RabbitMQ");
        return;
    }

    //std::cout << getCurrentTimestamp() << " Waiting for messages in queue: " << QUEUE_NAME << std::endl;
    LOG.info("Waiting for messages in queue: " + QUEUE_NAME, "RabbitMQ");

    while (true)
    {
        amqp_rpc_reply_t res;
        amqp_envelope_t envelope;

        amqp_maybe_release_buffers(conn);
        res = amqp_consume_message(conn, &envelope, nullptr, 0);

        if (res.reply_type == AMQP_RESPONSE_NORMAL)
        {
            std::string message_body((char*)envelope.message.body.bytes, envelope.message.body.len);
            //std::cout << getCurrentTimestamp() << " Received message: " << message_body << std::endl;
            LOG.info("Received message: " + message_body, "RabbitMQ");

            // Ack the message
            amqp_basic_ack(conn, CHANNEL_ID, envelope.delivery_tag, false);
            LOG.debug("Message acknowledged", "RabbitMQ");

            amqp_destroy_envelope(&envelope);
        } else{
            //std::cerr << getCurrentTimestamp() << " Failed to consume message" << std::endl;
            LOG.error("Failed to consume message", "RabbitMQ");
            break;
        }
        
    }

    LOG.info("Closing RabbitMQ connection", "RabbitMQ");
    amqp_channel_close(conn, CHANNEL_ID, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    
}

bool publishMessage(const std::string& message) {
    amqp_connection_state_t conn = amqp_new_connection();
    amqp_socket_t* socket = amqp_tcp_socket_new(conn);

    if(!socket) {
        //std::cerr << getCurrentTimestamp() << "Failed to create TCP socket" << std::endl;
        LOG.error("Failed to create TCP socket", "RabbitMQ");
        return false;
    }

    if(amqp_socket_open(socket, RABBITMQ_HOST.c_str(), RABBITMQ_PORT)) {
        //std::cerr << getCurrentTimestamp() << "Failed to open TCP connection" << std::endl;
        LOG.error("Failed to open TCP connection", "RabbitMQ");
        return false;
    }

    amqp_rpc_reply_t login_reply = amqp_login(
        conn,
        RABBITMQ_VHOST.c_str(),
        0,
        131072,
        0,
        AMQP_SASL_METHOD_PLAIN,
        RABBITMQ_USER.c_str(),
        RABBITMQ_PASSWORD.c_str()
    );

    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << "FAILED to log in to rabbitMQ" << std::endl;
        LOG.error("Failed to log in to RabbitMQ", "RabbitMQ");
        return false;
    }

    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t channel_type = amqp_get_rpc_reply(conn);
    if(channel_type.reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << "Failed to open a channel" << std::endl;
        LOG.error("Failed to open a channel", "RabbitMQ");
        return false;
    }

    amqp_queue_declare(
        conn,
        1,
        amqp_cstring_bytes(QUEUE_NAME.c_str()),
        0, // passive: 0 = create if not exists
        1, // durable: 1 = survive server restarts
        0, // exclusive: 0 = non-exclusive
        0, // auto-delete: 0 = do not auto-delete when not in use
        amqp_empty_table
    );

    if(amqp_get_rpc_reply(conn).reply_type != AMQP_RESPONSE_NORMAL) {
        //std::cerr << getCurrentTimestamp() << "Failed to declare queue" << std::endl;
        LOG.error("Failed to declare queue", "RabbitMQ");
        return false;
    }

    amqp_bytes_t message_bytes = amqp_cstring_bytes(message.c_str());
    int publish_status = amqp_basic_publish(
        conn,
        1,
        amqp_cstring_bytes(EXCHANGE_NAME.c_str()),
        amqp_cstring_bytes(ROUTING_KEY.c_str()),
        0,
        0,
        nullptr,
        message_bytes
    );

    if(publish_status < 0) {
        //std::cerr << getCurrentTimestamp() << "Failed to publish message" << std::endl;
        LOG.error("Failed to publish message", "RabbitMQ");
        return false;
    }

    //std::cout << getCurrentTimestamp() << "Message published successfully: " << message << std::endl;
    LOG.info("Message published successfully: " + message, "RabbitMQ");

    amqp_channel_close(conn, 1, AMQP_REPLY_SUCCESS);
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);

    return true;
}

std::string createJsonMessage(const std::string& file_name, const std::string& sync_time, const std::string& read_position) {
    std::stringstream ss;
    ss << "{\"temp_file_name\":\"" << file_name
       << "\", \"sync_time\":\"" << sync_time 
       << "\", \"read_position\":\"" << read_position << "\"}";
    return ss.str();
}

void executeRsync(const std::string& source, const std::string&  destination, const std::string& logfile) {
    // Get the current time with milliseconds for logging
    //struct timeval tv;
    //gettimeofday(&tv, nullptr);

    //char timestamp[64];
    //struct tm *tm_info = localtime(&tv.tv_sec);

    //strftime(timestamp, sizeof(timestamp), "%Y-%m-%d %H:%M:%S", tm_info);
    //snprintf(timestamp + strlen(timestamp), sizeof(timestamp) - strlen(timestamp), ".%03ld", tv.tv_usec / 1000);

    // Capture the start time
    auto start_time = std::chrono::high_resolution_clock::now();
    //std::cout << getCurrentTimestamp() << " - Starting rsync operation..." << std::endl;
    LOG.info("Starting rsync operation...", "Rsync");

    // Construct the rsync command with logging
    std::string command = "ionice -c1 -n0 nice -n -20 rsync -avz --no-perms --no-owner --no-group --update --append-verify "
                          " --inplace --progress --times --itemize-changes "
                          " --out-format='%i %n %M' --compress-level=1 --bwlimit=0 --blocking-io "
                          //" --log-file=" + logfile + " " + source + " " + destination;
                          //"--log-file=" + logfile + " "
                          //"--log-file-format='%t %o %i %n %M' "
                          + source + " " + destination;
    auto executeTime = getCurrentTimestamp();

    //std::cout << getCurrentTimestamp() << "Executing command: " << command << std::endl;
    LOG.debug("Executing command: " + command, "Rsync");

    // Execute the rsync command using popen
    //std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(command.c_str(), "r"), pclose);
    FILE* pipe = popen(command.c_str(), "r");
    if(!pipe){
        //std::cerr << "Failed to execute rsync command" << std::endl;
        LOG.error("Failed to execute rsync command", "Rsync");
        return;
    }

    char buffer[256];
    //std::regex regex(">f.* ([^ ]+)");
    std::regex regex(R"(>f.*\s([^\s]+)\s\d{4}/\d{2}/\d{2}-\d{2}:\d{2}:\d{2})");
    std::regex completion_regex(R"(\s(\d+(?:,\d+)*)\s100%\s+([0-9.]+[A-Z]B/s))");
    std::smatch match;
    std::string file_name;

    while (fgets(buffer, sizeof(buffer), pipe) != nullptr) 
    {
        std::string output(buffer);
        //std::cout << "Rsync Output: " << output;
        LOG.debug("Rsync Output: " + output, "Rsync");

        // Using regex in c++
        if(std::regex_search(output, match, regex) && match.size() > 1){
            file_name = match.str(1);
            LOG.debug("Matched File: " + file_name, "Rsync");
            //std::string message = createJsonMessage(file_name, executeTime);
            //std::cout << getCurrentTimestamp() << "Matched File: " << match.str(1) << std::endl;
            //std::cout << getCurrentTimestamp() << "Generated JSON Message: " << message << std::endl;
            //publishMessage(message);
        }

        // Using regex to detect '100%' completion and get transferred bytes
        if(std::regex_search(output, match, completion_regex) && match.size() > 2) {
            std::string transferred_bytes = match.str(1); // Extract transferred bytes
            std::string transfer_speed = match.str(2); // Extract transfer speed

            //std::cout << getCurrentTimestamp() << "Read position: " << transferred_bytes << std::endl;
            LOG.info("Read position: " + transferred_bytes + " at " + transfer_speed, "Rsync");
            std::string message = createJsonMessage(file_name, executeTime, transferred_bytes);
            //std::cout << getCurrentTimestamp() << "Generated JSON Message: " << message << std::endl;
            LOG.info("Generated JSON Message: " + message, "Rsync");
            publishMessage(message);
        }
    }

    // Get the exit status
    int result = pclose(pipe);
    if(result == -1) {
        //std::cerr << getCurrentTimestamp() << " - Error closing the command pipe" << std::endl;
        LOG.error("Error closing the command pipe", "Rsync");
    } else {
        //std::cout << getCurrentTimestamp() << " - Rsync completed with exit code: " << WEXITSTATUS(result) << std::endl;
        LOG.info("Rsync completed with exit code: " + std::to_string(WEXITSTATUS(result)), "Rsync");
    }
    
    // Calculate and display the execution duration
    auto end_time = std::chrono::high_resolution_clock::now();
    std::chrono::duration<double, std::milli> execution_duration = end_time - start_time;

    //std::cout << getCurrentTimestamp() << " - Rsync operation completed in " 
    //          << std::fixed << std::setprecision(3) 
    //          << execution_duration.count() << " ms" << std::endl;
    LOG.info("Rsync operation completed in " + 
        std::to_string(execution_duration.count()) + " ms", "Rsync");
}

// Function to read part of an STDF file and extract PRR records
int main() {
    const std::string source = "rsync://LPX-67/user/UTAC_UTACS1-S_WP90-73509-11A_WS8P-73509-11A-01P0_7R4C395.000-7R4C395.22E7_20250227134121_0_WS1.stdf_open";
    const std::string destination = "/tmp/IFLEX-18/";
    const std::string logfile = "/tmp/IFLEX-18/Logs/rsync_log_IFLEX-LPX-67.txt";

    // Initialize Logger
    std::string appLogPath = "/tmp/IFLEX-18/Logs/application_IFLEX-67.log";
    LOG.init(appLogPath, LogLevel::DEBUG);
    LOG.info("Application starting....", "Main");

    // Create thread for message consumption
    std::thread consumer_thread([]() {
        try{
            LOG.info("Starting consumer thread", "Consumer");
            consumeMessages();
        } catch (const std::exception& e){
            //std::cerr << "Error while consuming messages: " << e.what() << std::endl;
            LOG.error("Error while consuming messages: " + std::string(e.what()), "Consumer");
        }
    });

    // Create thread for rsync execution
    std::thread rsync_thread([source, destination, logfile]() {
        LOG.info("Starting rsync thread", "Rsync");
        while(true) {
            try{
                executeRsync(source, destination, logfile);
            } catch (const std::exception& e){
                //std::cerr << "Error during rsync execution: " << e.what() << std::endl;
                LOG.error("Error during rsync execution: " + std::string(e.what()), "Rsync");
            }

            // Wait for 1 millisecond before the next execution using C++ std::this_thread
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
    });

    LOG.info("Threads started, waiting for completion", "Main");

    // Wait for threads to complete(though they should run indefinitely)
    consumer_thread.join();
    rsync_thread.join();

    LOG.info("Application shutting down", "Main");
    
    return 0;
}