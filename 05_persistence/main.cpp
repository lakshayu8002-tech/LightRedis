#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <sstream>
#include <cstring>
#include <unistd.h>
#include <fcntl.h>
#include <fstream>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <netinet/in.h>

using namespace std;

const int MAX_EVENTS = 64;

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
    unordered_map<string, string> db;

    ifstream infile("database.txt");
    if (infile.is_open()) {
        string saved_key, saved_val;
        while (infile >> saved_key) {
            infile.get(); 
            getline(infile, saved_val); 
            db[saved_key] = saved_val;
        }
        infile.close();
        cout << "[Startup] Successfully reloaded database from hard drive!" << endl;
    }

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

    cout << "Persistent Multi-Client Engine Active on Port 6379..." << endl;

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
                    db[args[1]] = args[2];
                    response = "+OK\r\n";

                    // --- PERSISTENCE WRITE ---
                    // Open file in append-mode and log the entry safely
                    ofstream outfile("database.txt", ios::app);
                    if (outfile.is_open()) {
                        outfile << args[1] << " " << args[2] << "\n";
                        outfile.close();
                    }

                } else if (cmd == "GET" && args.size() >= 2) {
                    auto it = db.find(args[1]);
                    if (it != db.end()) {
                        response = "$" + to_string(it->second.length()) + "\r\n" + it->second + "\r\n";
                    } else {
                        response = "$-1\r\n";
                    }
                } else if (cmd == "DEL" && args.size() >= 2) {
                    if (db.erase(args[1])) {
                        response = ":1\r\n";
                        
                        // Clear out the file and write the current live map state
                        ofstream outfile("database.txt");
                        for (auto const& [key, val] : db) {
                            outfile << key << " " << val << "\n";
                        }
                        outfile.close();
                    } else {
                        response = ":0\r\n";
                    }
                } else {
                    response = "-ERR Unknown or incomplete command\r\n";
                }

                send(client_fd, response.c_str(), response.length(), 0);
            }
        }
    }

    close(server_fd);
    return 0;
}