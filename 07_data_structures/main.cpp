#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <deque>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <ctime>
#include <fstream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>

using namespace std;

const int MAX_EVENTS = 64;

struct RedisObject {
    string type;
    string string_val;
    deque<string> list_val; 
    time_t expire_at = 0;
};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

void sync_db(const unordered_map<string, RedisObject>& db) {
    ofstream outfile("database.txt");
    if (!outfile.is_open()) return;
    for (auto const& [key, obj] : db) {
        if (obj.expire_at != 0 && time(nullptr) >= obj.expire_at) continue;
        outfile << obj.type << "\n" << key << "\n" << obj.expire_at << "\n";
        if (obj.type == "string") {
            outfile << obj.string_val << "\n";
        } else if (obj.type == "list") {
            outfile << obj.list_val.size() << "\n";
            for (const auto& item : obj.list_val) {
                outfile << item << "\n";
            }
        }
    }
    outfile.close();
}

vector<string> parse_resp(const string& input) {
    vector<string> commands;
    if (input.empty() || input[0] != '*') return commands;
    try {
        size_t pos = 1;
        size_t next_crlf = input.find("\r\n", pos);
        if (next_crlf == string::npos) return commands;
        int num_elements = stoi(input.substr(pos, next_crlf - pos));
        pos = next_crlf + 2;
        for (int i = 0; i < num_elements; ++i) {
            if (pos >= input.size() || input[pos] != '$') break;
            pos++;
            next_crlf = input.find("\r\n", pos);
            if (next_crlf == string::npos) break;
            int len = stoi(input.substr(pos, next_crlf - pos));
            pos = next_crlf + 2;
            if (pos + len > input.size()) break;
            commands.push_back(input.substr(pos, len));
            pos += len + 2;
        }
    } catch (...) {
        commands.clear();
    }
    return commands;
}

int main() {
    unordered_map<string, RedisObject> db;

    // Load database safely, ensuring multi-line format compatibility
    ifstream infile("database.txt");
    if (infile.is_open()) {
        string type, key, exp_str;
        while (getline(infile, type)) {
            if (type.find(' ') != string::npos) {
                // Skips old flat Stage 06 layout lines to prevent database corruption
                continue; 
            }
            if (!getline(infile, key)) break;
            if (!getline(infile, exp_str)) break;
            try {
                time_t exp = stol(exp_str);
                if (exp != 0 && time(nullptr) >= exp) {
                    if (type == "string") {
                        string dummy;
                        getline(infile, dummy);
                    } else if (type == "list") {
                        string size_str;
                        getline(infile, size_str);
                        int size = stoi(size_str);
                        for (int i = 0; i < size; ++i) {
                            string dummy;
                            getline(infile, dummy);
                        }
                    }
                    continue;
                }
                RedisObject obj;
                obj.type = type;
                obj.expire_at = exp;
                if (type == "string") {
                    getline(infile, obj.string_val);
                } else if (type == "list") {
                    string size_str;
                    getline(infile, size_str);
                    int size = stoi(size_str);
                    for (int j = 0; j < size; ++j) {
                        string val;
                        getline(infile, val);
                        obj.list_val.push_back(val);
                    }
                }
                db[key] = obj;
            } catch (...) {
                continue;
            }
        }
        infile.close();
        cout << "[Startup] Successfully reloaded database from hard drive!" << endl;
    }

    auto check_ttl = [&](const string& key) -> bool {
        auto it = db.find(key);
        if (it != db.end()) {
            if (it->second.expire_at != 0 && time(nullptr) >= it->second.expire_at) {
                db.erase(it);
                return true;
            }
        }
        return false;
    };

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) return 1;

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr*)&address, sizeof(address)) < 0) return 1;
    if (listen(server_fd, 10) < 0) return 1;

    set_nonblocking(server_fd);

    int epoll_fd = epoll_create1(0);
    if (epoll_fd < 0) return 1;

    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) < 0) return 1;

    cout << "Unified Cumulative Engine (Stage 07) Active on Port 6379..." << endl;

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
                }
            } else {
                int client_fd = events[i].data.fd;
                char buffer[4096];
                memset(buffer, 0, sizeof(buffer));
                int bytes_received = read(client_fd, buffer, sizeof(buffer) - 1);
                if (bytes_received <= 0) {
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, client_fd, nullptr);
                    close(client_fd);
                    continue;
                }
                string input(buffer, bytes_received);
                vector<string> args;
                if (input[0] == '*') {
                    args = parse_resp(input);
                } else {
                    string current;
                    bool in_quotes = false;
                    for (char c : input) {
                        if (c == '"') {
                            in_quotes = !in_quotes;
                        } else if ((c == ' ' || c == '\r' || c == '\n') && !in_quotes) {
                            if (!current.empty()) {
                                args.push_back(current);
                                current.clear();
                            }
                        } else {
                            current.push_back(c);
                        }
                    }
                    if (!current.empty()) args.push_back(current);
                }
                if (args.empty()) {
                    string err = "-ERR Protocol Error\r\n";
                    send(client_fd, err.c_str(), err.length(), 0);
                    continue;
                }
                string cmd = args[0];
                string response;
                bool db_changed = false;
                if (args.size() >= 2) {
                    if (check_ttl(args[1])) {
                        db_changed = true;
                    }
                }
                if (cmd == "SET" && args.size() >= 3) {
                    RedisObject obj;
                    obj.type = "string";
                    obj.string_val = args[2];
                    obj.expire_at = 0;
                    db[args[1]] = obj;
                    response = "+OK\r\n";
                    db_changed = true;
                } else if (cmd == "GET" && args.size() >= 2) {
                    auto it = db.find(args[1]);
                    if (it != db.end()) {
                        if (it->second.type != "string") {
                            response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                        } else {
                            response = "$" + to_string(it->second.string_val.length()) + "\r\n" + it->second.string_val + "\r\n";
                        }
                    } else {
                        response = "$-1\r\n";
                    }
                } else if (cmd == "LPUSH" && args.size() >= 3) {
                    auto& obj = db[args[1]];
                    if (obj.type.empty()) {
                        obj.type = "list";
                        obj.expire_at = 0;
                    }
                    if (obj.type != "list") {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    } else {
                        for (size_t j = 2; j < args.size(); ++j) {
                            obj.list_val.push_front(args[j]);
                        }
                        response = ":" + to_string(obj.list_val.size()) + "\r\n";
                        db_changed = true;
                    }
                } else if (cmd == "LRANGE" && args.size() >= 4) {
                    auto it = db.find(args[1]);
                    if (it == db.end()) {
                        response = "*0\r\n";
                    } else if (it->second.type != "list") {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    } else {
                        try {
                            int start = stoi(args[2]);
                            int stop = stoi(args[3]);
                            int list_size = it->second.list_val.size();
                            if (start < 0) start = list_size + start;
                            if (stop < 0) stop = list_size + stop;
                            if (start < 0) start = 0;
                            if (stop >= list_size) stop = list_size - 1;
                            if (start > stop || start >= list_size) {
                                response = "*0\r\n";
                            } else {
                                int count = stop - start + 1;
                                string res = "*" + to_string(count) + "\r\n";
                                for (int j = start; j <= stop; ++j) {
                                    string val = it->second.list_val[j];
                                    res += "$" + to_string(val.length()) + "\r\n" + val + "\r\n";
                                }
                                response = res;
                            }
                        } catch (...) {
                            response = "-ERR value is not an integer or out of range\r\n";
                        }
                    }
                } else if (cmd == "EXPIRE" && args.size() >= 3) {
                    auto it = db.find(args[1]);
                    if (it != db.end()) {
                        try {
                            int seconds = stoi(args[2]);
                            it->second.expire_at = time(nullptr) + seconds;
                            response = ":1\r\n";
                            db_changed = true;
                        } catch (...) {
                            response = "-ERR value is not an integer or out of range\r\n";
                        }
                    } else {
                        response = ":0\r\n";
                    }
                } else if (cmd == "TTL" && args.size() >= 2) {
                    auto it = db.find(args[1]);
                    if (it != db.end()) {
                        if (it->second.expire_at == 0) {
                            response = ":-1\r\n";
                        } else {
                            time_t remaining = it->second.expire_at - time(nullptr);
                            if (remaining <= 0) {
                                db.erase(it);
                                response = ":-2\r\n";
                                db_changed = true;
                            } else {
                                response = ":" + to_string(remaining) + "\r\n";
                            }
                        }
                    } else {
                        response = ":-2\r\n";
                    }
                } else if (cmd == "DEL" && args.size() >= 2) {
                    if (db.erase(args[1])) {
                        response = ":1\r\n";
                        db_changed = true;
                    } else {
                        response = ":0\r\n";
                    }
                } else {
                    response = "-ERR Unknown or incomplete command\r\n";
                }
                if (db_changed) {
                    sync_db(db);
                }
                send(client_fd, response.c_str(), response.length(), 0);
            }
        }
    }
    close(server_fd);
    return 0;
}