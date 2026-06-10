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
#include <arpa/inet.h>

using namespace std;

const int MAX_EVENTS = 64;

void set_nonblocking(int fd) {
    int flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
}

int main(int argc, char* argv[]) {
    int port = 6379;
    bool is_slave = false;
    string master_host = "";
    int master_port = 0;

    if (argc >= 2) {
        port = stoi(argv[1]);
    }
    if (argc >= 4 && string(argv[2]) == "--replicaof") {
        is_slave = true;
        master_host = argv[3];
        master_port = stoi(argv[4]);
    }

    unordered_map<string, string> db;
    vector<int> slaves;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    sockaddr_in address;
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port);

    bind(server_fd, (struct sockaddr*)&address, sizeof(address));
    listen(server_fd, 10);
    set_nonblocking(server_fd);

    int epoll_fd = epoll_create1(0);
    struct epoll_event ev, events[MAX_EVENTS];
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;
    epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev);

    if (is_slave) {
        int m_fd = socket(AF_INET, SOCK_STREAM, 0);
        sockaddr_in master_addr;
        master_addr.sin_family = AF_INET;
        master_addr.sin_port = htons(master_port);
        inet_pton(AF_INET, master_host.c_str(), &master_addr.sin_addr);
        
        if (connect(m_fd, (struct sockaddr*)&master_addr, sizeof(master_addr)) >= 0) {
            set_nonblocking(m_fd);
            ev.events = EPOLLIN;
            ev.data.fd = m_fd;
            epoll_ctl(epoll_fd, EPOLL_CTL_ADD, m_fd, &ev);
            cout << "Slave running on port " << port << " connected to Master!" << endl;
        }
    } else {
        cout << "Master running on port " << port << "..." << endl;
    }

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
                    if (!is_slave) {
                        slaves.push_back(client_fd);
                    }
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
                stringstream ss(input);
                vector<string> args;
                string temp;
                while (ss >> temp) args.push_back(temp);

                if (args.empty()) continue;
                string cmd = args[0];
                string response = "";

                if (cmd == "SET" && args.size() >= 3) {
                    db[args[1]] = args[2];
                    response = "+OK\r\n";
                    
                    if (!is_slave) {
                        string sync_cmd = "SET " + args[1] + " " + args[2] + "\n";
                        for (int slave_fd : slaves) {
                            send(slave_fd, sync_cmd.c_str(), sync_cmd.length(), 0);
                        }
                    }
                } else if (cmd == "GET" && args.size() >= 2) {
                    auto it = db.find(args[1]);
                    if (it != db.end()) response = "$" + to_string(it->second.length()) + "\r\n" + it->second + "\r\n";
                    else response = "$-1\r\n";
                }

                if (!response.empty()) {
                    send(client_fd, response.c_str(), response.length(), 0);
                }
            }
        }
    }
    close(server_fd);
    return 0;
}