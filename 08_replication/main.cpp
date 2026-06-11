#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <chrono>
#include <cstring>
#include <algorithm>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>
#include <arpa/inet.h>

using namespace std;

const int MAX_EVENTS = 64;
const string MASTER_REPLID = "8371b4fb1155b71f4a04d3d1bc3e18c4a990aeeb";

struct DBValue {
    string value;
    chrono::steady_clock::time_point expiry;
    bool has_expiry = false;
};

enum class ConnType { CLIENT, PENDING_REPLICA, CONFIRMED_REPLICA, MASTER_LINK };

struct Connection {
    int fd;
    string buffer;
    ConnType type;
    int handshake_state = 0;
};

unordered_map<string, DBValue> db;
unordered_map<string, vector<string>> db_lists;
unordered_map<int, Connection> connections;
vector<int> replicas; 
bool is_slave = false;
string master_host = "";
int master_port = 0;
int my_port = 6379;

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

string hex_to_bytes(const string& hex) {
    string bytes = "";
    for (size_t i = 0; i < hex.length(); i += 2) {
        string byteString = hex.substr(i, 2);
        char byte = (char)strtol(byteString.c_str(), nullptr, 16);
        bytes.push_back(byte);
    }
    return bytes;
}

string get_empty_rdb() {
    string hex_rdb = "524544495330303131fa0972656469732d76657205372e322e34fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000ff10aa11a4cf13543e";
    string binary_rdb = hex_to_bytes(hex_rdb);
    return "$" + to_string(binary_rdb.length()) + "\r\n" + binary_rdb;
}

string format_resp_array(const vector<string>& args) {
    string res = "*" + to_string(args.size()) + "\r\n";
    for (const auto& arg : args) {
        res += "$" + to_string(arg.length()) + "\r\n" + arg + "\r\n";
    }
    return res;
}

bool parse_resp_commands(string& buffer, vector<vector<string>>& out_commands) {
    size_t pos = 0;
    while (pos < buffer.length()) {
        if (buffer[pos] != '*') {
            pos++;
            continue;
        }
        
        size_t rn = buffer.find("\r\n", pos);
        if (rn == string::npos) break;
        
        int num_elements = 0;
        try {
            num_elements = stoi(buffer.substr(pos + 1, rn - (pos + 1)));
        } catch (...) {
            pos = rn + 2;
            continue;
        }

        size_t current_pos = rn + 2;
        vector<string> args;
        bool incomplete = false;
        
        for (int i = 0; i < num_elements; ++i) {
            if (current_pos >= buffer.length() || buffer[current_pos] != '$') {
                incomplete = true; break;
            }
            size_t len_rn = buffer.find("\r\n", current_pos);
            if (len_rn == string::npos) { incomplete = true; break; }
            
            int str_len = 0;
            try {
                str_len = stoi(buffer.substr(current_pos + 1, len_rn - (current_pos + 1)));
            } catch (...) {
                incomplete = true; break;
            }

            current_pos = len_rn + 2;
            if (current_pos + str_len + 2 > buffer.length()) {
                incomplete = true; break;
            }
            
            args.push_back(buffer.substr(current_pos, str_len));
            current_pos += str_len + 2;
        }
        
        if (incomplete) break;
        out_commands.push_back(args);
        pos = current_pos;
    }
    
    if (pos > 0) {
        buffer = buffer.substr(pos);
    }
    return !out_commands.empty();
}

void handle_redis_command(int fd, const vector<string>& args, int epoll_fd) {
    if (args.empty()) return;
    
    string cmd = args[0];
    for (char &c : cmd) c = toupper(c);
    
    string response = "";
    bool write_command = false;

    if (cmd == "PING") {
        response = "+PONG\r\n";
    } 
    else if (cmd == "ECHO" && args.size() > 1) {
        response = "$" + to_string(args[1].length()) + "\r\n" + args[1] + "\r\n";
    } 
    else if (cmd == "SET" && args.size() >= 3) {
        DBValue val;
        val.value = args[2];
        if (args.size() >= 5) {
            string option = args[3];
            for (char &c : option) c = toupper(c);
            if (option == "PX") {
                try {
                    long long ms = stoll(args[4]);
                    val.expiry = chrono::steady_clock::now() + chrono::milliseconds(ms);
                    val.has_expiry = true;
                } catch (...) {}
            }
        }
        db[args[1]] = val;
        response = "+OK\r\n";
        write_command = true;
    } 
    else if (cmd == "GET" && args.size() > 1) {
        auto it = db.find(args[1]);
        if (it != db.end()) {
            if (it->second.has_expiry && chrono::steady_clock::now() > it->second.expiry) {
                db.erase(it);
                response = "$-1\r\n";
            } else {
                response = "$" + to_string(it->second.value.length()) + "\r\n" + it->second.value + "\r\n";
            }
        } else {
            response = "$-1\r\n";
        }
    }
    else if (cmd == "LPUSH" && args.size() > 2) {
        auto& list = db_lists[args[1]];
        for (size_t i = 2; i < args.size(); ++i) {
            list.insert(list.begin(), args[i]);
        }
        response = ":" + to_string(list.size()) + "\r\n";
        write_command = true;
    } 
    else if (cmd == "LRANGE" && args.size() == 4) {
        auto it = db_lists.find(args[1]);
        if (it == db_lists.end()) {
            response = "*0\r\n";
        } else {
            const auto& list = it->second;
            int start = stoi(args[2]);
            int end = stoi(args[3]);
            
            if (start < 0) start = max(0, (int)list.size() + start);
            if (end < 0) end = max(0, (int)list.size() + end);
            if (end >= (int)list.size()) end = list.size() - 1;
            
            if (start > end || start >= (int)list.size()) {
                response = "*0\r\n";
            } else {
                vector<string> sub_list(list.begin() + start, list.begin() + end + 1);
                response = format_resp_array(sub_list);
            }
        }
    }
    else if (cmd == "INFO" && args.size() > 1) {
        string sub = args[1];
        for (char &c : sub) c = toupper(c);
        if (sub == "REPLICATION") {
            string info_content = is_slave ? "role:slave\r\n" : "role:master\r\n";
            info_content += "master_replid:" + MASTER_REPLID + "\r\n";
            info_content += "master_repl_offset:0\r\n";
            response = "$" + to_string(info_content.length()) + "\r\n" + info_content + "\r\n";
        }
    } 
    else if (cmd == "REPLCONF") {
        response = "+OK\r\n";
    } 
    else if (cmd == "PSYNC") {
        connections[fd].type = ConnType::CONFIRMED_REPLICA;
        replicas.push_back(fd);
        
        string resync_meta = "+FULLRESYNC " + MASTER_REPLID + " 0\r\n";
        send(fd, resync_meta.c_str(), resync_meta.length(), 0);
        response = get_empty_rdb();
    }

    if (!is_slave && write_command) {
        string propagated_resp = format_resp_array(args);
        for (int replica_fd : replicas) {
            send(replica_fd, propagated_resp.c_str(), propagated_resp.length(), 0);
        }
    }

    if (connections[fd].type == ConnType::MASTER_LINK && write_command) {
        return; 
    }

    if (!response.empty()) {
        send(fd, response.c_str(), response.length(), 0);
    }
}

void run_slave_handshake(int m_fd) {
    Connection& conn = connections[m_fd];
    if (conn.handshake_state == 0) {
        string ping = format_resp_array({"PING"});
        send(m_fd, ping.c_str(), ping.length(), 0);
        conn.handshake_state = 1;
    } else if (conn.handshake_state == 1 && conn.buffer.find("+PONG\r\n") != string::npos) {
        conn.buffer.clear();
        string replconf1 = format_resp_array({"REPLCONF", "listening-port", to_string(my_port)});
        send(m_fd, replconf1.c_str(), replconf1.length(), 0);
        conn.handshake_state = 2;
    } else if (conn.handshake_state == 2 && conn.buffer.find("+OK\r\n") != string::npos) {
        conn.buffer.clear();
        string replconf2 = format_resp_array({"REPLCONF", "capa", "psync2"});
        send(m_fd, replconf2.c_str(), replconf2.length(), 0);
        conn.handshake_state = 3;
    } else if (conn.handshake_state == 3 && conn.buffer.find("+OK\r\n") != string::npos) {
        conn.buffer.clear();
        string psync = format_resp_array({"PSYNC", "?", "-1"});
        send(m_fd, psync.c_str(), psync.length(), 0);
        conn.handshake_state = 4;
    } else if (conn.handshake_state == 4) {
        size_t fullresync_pos = conn.buffer.find("+FULLRESYNC");
        if (fullresync_pos != string::npos) {
            size_t first_rn = conn.buffer.find("\r\n", fullresync_pos);
            if (first_rn != string::npos) {
                size_t dollar_pos = conn.buffer.find("$", first_rn);
                if (dollar_pos != string::npos) {
                    size_t second_rn = conn.buffer.find("\r\n", dollar_pos);
                    if (second_rn != string::npos) {
                        try {
                            int rdb_len = stoi(conn.buffer.substr(dollar_pos + 1, second_rn - (dollar_pos + 1)));
                            size_t rdb_start_pos = second_rn + 2;
                            if (conn.buffer.length() >= rdb_start_pos + rdb_len) {
                                conn.buffer = conn.buffer.substr(rdb_start_pos + rdb_len);
                                conn.handshake_state = 5;
                                cout << "Handshake fully synchronized with master successfully." << endl;
                            }
                        } catch (...) {
                            conn.buffer.clear();
                        }
                    }
                }
            }
        }
    }
}

int main(int argc, char* argv[]) {
    for (int i = 1; i < argc; ++i) {
        string arg = argv[i];
        if (arg == "--port") {
            if (i + 1 < argc) {
                try {
                    my_port = stoi(argv[++i]);
                } catch (...) {
                    cerr << "Invalid port specified." << endl;
                    return 1;
                }
            }
        } else if (arg == "--replicaof") {
            if (i + 2 < argc) {
                is_slave = true;
                master_host = argv[++i];
                try {
                    master_port = stoi(argv[++i]);
                } catch (...) {
                    cerr << "Invalid master port specified." << endl;
                    return 1;
                }
            }
        }
    }

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(my_port);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) {
        cerr << "Bind failed" << endl; return 1;
    }
    listen(server_fd, 10);
    set_nonblocking(server_fd);

    int epoll_fd = epoll_create1(0);
    struct epoll_event ev{}, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    if (is_slave) {
        int m_fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in master_addr{};
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(master_port);
        inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);
        
        if (connect(m_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) >= 0) {
            set_nonblocking(m_fd);
            ev.events = EPOLLIN;
            ev.data.fd = m_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, m_fd, &ev);
            
            Connection master_conn{m_fd, "", ConnType::MASTER_LINK, 0};
            connections[m_fd] = master_conn;
            
            run_slave_handshake(m_fd);
        }
    }

    cout << (is_slave ? "Replica" : "Master") << " server running on port " << my_port << "..." << endl;

    while (true) {
        int nfds = epoll_wait(epoll_fd, events, MAX_EVENTS, -1);
        for (int i = 0; i < nfds; ++i) {
            if (events[i].data.fd == server_fd) {
                int client_fd = accept(server_fd, nullptr, nullptr);
                if (client_fd >= 0) {
                    set_nonblocking(client_fd);
                    ev.events = EPOLLIN;
                    ev.data.fd = client_fd;
                    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &ev);
                    
                    connections[client_fd] = Connection{client_fd, "", ConnType::CLIENT, 0};
                }
            } else {
                int target_fd = events[i].data.fd;
                char read_raw[4096];
                int bytes_received = read(target_fd, read_raw, sizeof(read_raw));

                if (bytes_received <= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, target_fd, nullptr);
                    close(target_fd);
                    connections.erase(target_fd);
                    replicas.erase(remove(replicas.begin(), replicas.end(), target_fd), replicas.end());
                    continue;
                }

                connections[target_fd].buffer.append(read_raw, bytes_received);

                if (connections[target_fd].type == ConnType::MASTER_LINK && connections[target_fd].handshake_state < 5) {
                    run_slave_handshake(target_fd);
                    if (connections[target_fd].handshake_state < 5) {
                        continue; 
                    }
                }

                vector<vector<string>> parsed_commands;
                if (parse_resp_commands(connections[target_fd].buffer, parsed_commands)) {
                    for (const auto& cmd_tokens : parsed_commands) {
                        handle_redis_command(target_fd, cmd_tokens, epoll_fd);
                    }
                }
            }
        }
    }
    close(server_fd);
    return 0;
}