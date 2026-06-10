#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

using namespace std;

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

    cout << "Single-Client RESP Engine Active on Port 6379..." << endl;

    while (true) {
        int client_fd = accept(server_fd, nullptr, nullptr);
        if (client_fd < 0) continue;

        char buffer[4096];
        while (true) {
            memset(buffer, 0, sizeof(buffer));
            int bytes_received = read(client_fd, buffer, sizeof(buffer) - 1);
            
            if (bytes_received <= 0) {
                close(client_fd);
                break;
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
            } else if (cmd == "GET" && args.size() >= 2) {
                auto it = db.find(args[1]);
                if (it != db.end()) {
                    response = "$" + to_string(it->second.length()) + "\r\n" + it->second + "\r\n";
                } else {
                    response = "$-1\r\n";
                }
            } else if (cmd == "DEL" && args.size() >= 2) {
                if (db.erase(args[1])) response = ":1\r\n";
                else response = ":0\r\n";
            } else {
                response = "-ERR Unknown or incomplete command\r\n";
            }

            send(client_fd, response.c_str(), response.length(), 0);
        }
    }

    close(server_fd);
    return 0;
}