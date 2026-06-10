#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>
#include <cstring>
#include <unistd.h>
#include <sys/socket.h>
#include <netinet/in.h>

using namespace std;

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

    cout << "Single-Client Network Portal Active on Port 6379..." << endl;

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

            if (args.empty()) {
                string err = "ERR Protocol Error\n";
                send(client_fd, err.c_str(), err.length(), 0);
                continue;
            }

            string cmd = args[0];
            string response;

            if (cmd == "SET" && args.size() >= 3) {
                db[args[1]] = args[2];
                response = "OK\n";
            } else if (cmd == "GET" && args.size() >= 2) {
                auto it = db.find(args[1]);
                if (it != db.end()) {
                    response = "\"" + it->second + "\"\n";
                } else {
                    response = "(nil)\n";
                }
            } else if (cmd == "DEL" && args.size() >= 2) {
                if (db.erase(args[1])) response = "(integer) 1\n";
                else response = "(integer) 0\n";
            } else {
                response = "ERR Unknown or incomplete command\n";
            }

            send(client_fd, response.c_str(), response.length(), 0);
        }
    }

    close(server_fd);
    return 0;
}