#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>

using namespace std;

const int MAX_EVENTS = 64;

struct RedisObject {
    string type;
    string string_val;
    vector<string> list_val;
};

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

vector<string> parse_resp(const string& input) {
    vector<string> commands;
    if (input.empty() || input[0] != '*') return commands;

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
    return commands;
}

int main() {
    unordered_map<string, RedisObject> db;

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

    cout << "Data Structures Engine Active on Port 6379..." << endl;

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
            } 
            else {
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
                        if (c == '\r' || c == '\n') continue;
                        if (c == '"') {
                            in_quotes = !in_quotes;
                            continue;
                        }
                        if (c == ' ' && !in_quotes) {
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

                if (cmd == "SET" && args.size() >= 3) {
                    RedisObject obj;
                    obj.type = "string";
                    obj.string_val = args[2];
                    db[args[1]] = obj;
                    response = "+OK\r\n";
                } 
                else if (cmd == "GET" && args.size() >= 2) {
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
                } 
                else if (cmd == "LPUSH" && args.size() >= 3) {
                    auto& obj = db[args[1]];
                    if (obj.type.empty()) {
                        obj.type = "list";
                    }
                    
                    if (obj.type != "list") {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    } else {
                        obj.list_val.insert(obj.list_val.begin(), args[2]);
                        response = ":" + to_string(obj.list_val.size()) + "\r\n";
                    }
                } 
                else if (cmd == "LRANGE" && args.size() >= 4) {
                    auto it = db.find(args[1]);
                    if (it == db.end()) {
                        response = "*0\r\n";
                    } else if (it->second.type != "list") {
                        response = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n";
                    } else {
                        int start = stoi(args[2]);
                        int stop = stoi(args[3]);
                        int list_size = it->second.list_val.size();

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
                    }
                } 
                else if (cmd == "DEL" && args.size() >= 2) {
                    if (db.erase(args[1])) response = ":1\r\n";
                    else response = ":0\r\n";
                } 
                else {
                    response = "-ERR Unknown or incomplete command\r\n";
                }

                send(client_fd, response.c_str(), response.length(), 0);
            }
        }
    }

    close(server_fd);
    return 0;
}