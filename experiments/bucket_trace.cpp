#include <iostream>
#include <unordered_map>
#include <string>

using namespace std;

int main() {
    unordered_map<string, string> db;

    cout << "Initial Buckets: " << db.bucket_count() << endl;
    cout << "------------------------------------------------" << endl;

    for (int i = 1; i <= 20; ++i) {
        string key = "key_" + to_string(i);
        db[key] = "value";

        cout << "Elements: " << db.size() 
             << " | Buckets: " << db.bucket_count() 
             << " | Load Factor: " << db.load_factor() << endl;
    }

    return 0;
}