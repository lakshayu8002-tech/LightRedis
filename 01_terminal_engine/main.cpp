#include <iostream>
#include <unordered_map>
#include <string>
#include <vector>

using namespace std;

int main() {
    unordered_map<string, string> db;
    string line;

    cout << "Terminal Storage Engine Active. Type your queries..." << endl;

    while (true) {
        cout << "> ";
        if (!getline(cin, line)) break;

        if (!line.empty() && line.back() == '\r') {
            line.pop_back();
        }

        vector<string> args;
        string current;
        bool in_quotes = false;

        for (char c : line) {
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

        if (args.empty()) continue;

        string cmd = args[0];

        if (cmd == "SET" && args.size() >= 3) {
            db[args[1]] = args[2];
            cout << "OK" << endl;
        } else if (cmd == "GET" && args.size() >= 2) {
            auto it = db.find(args[1]);
            if (it != db.end()) {
                cout << "\"" << it->second << "\"" << endl;
            } else {
                cout << "(nil)" << endl;
            }
        } else if (cmd == "DEL" && args.size() >= 2) {
            if (db.erase(args[1])) {
                cout << "(integer) 1" << endl;
            } else {
                cout << "(integer) 0" << endl;
            }
        } else if (cmd == "EXIT") {
            break;
        } else {
            cout << "ERR Unknown or incomplete command" << endl;
        }
    }
    return 0;
}