#include <iostream>
#include <fstream>
#include "csv.hpp"
#include <libpq-fe.h>

using namespace std;
using namespace csv;

string trim(const string& s) {
    size_t start = s.find_first_not_of(" \t\n\r");
    if (start == string::npos) { return ""; }

    size_t end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

string cleanString(const string& input) {
    string s = trim(input);
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') {
        s = s.substr(1, s.size() - 2);
    }
    return s;
}

string build_con_string() {
    const char* host = getenv("PGHost");
    const char* port = getenv("PGPort");
    const char* dbName = getenv("PGDBName");
    const char* user = getenv("PGUser");
    const char* password = getenv("PGPW");
    const char* sslMode = getenv("PGSSLMODE");

    if (!host || !port || !dbName || !user || !password) {
        cerr << "Missing DB environment variables" << endl;
        exit(1);
    }

    string conStr = "host=" + string(host) +
                    " port=" + string(port) +
                    " dbname=" + string(dbName) +
                    " user=" + string(user) +
                    " password=" + string(password);

    if (sslMode) {
        conStr += " sslmode=" + string(sslMode);
    }
    return conStr;
}

string escapeSQLString(const string& input) {
    string output;
    for (char c : input) {
        if (c == '\'') { output += "''"; }
        else { output += c; }
    }
    return output;
}

int main() {
    // Build connection string and connect
    string conInfo = build_con_string();
    PGconn* con = PQconnectdb(conInfo.c_str());

    if (PQstatus(con) != CONNECTION_OK) {
        cerr << "Connection to database failed: " << PQerrorMessage(con);
        PQfinish(con);
        return 1;
    }
    cout << "Connected to Database" << endl;

    try {
        CSVReader reader("Example.csv");

        if (reader.get_col_names().empty()) {
            cout << "No header" << endl;
        } else {
            cout << "Headers detected:" << endl;
            for (auto& name : reader.get_col_names()) {
                cout << " - " << name << endl;
            }
        }

        // Insert rows into DB
        for (CSVRow& row : reader) {
            string query = "INSERT INTO csv_data VALUES(";

            for (size_t i = 0; i < row.size(); ++i) {
                string cleaned = cleanString(row[i].get<string>());
                string escaped = escapeSQLString(cleaned);
                query += "'" + escaped + "'";
                if (i < row.size() - 1) query += ", ";
            }
            query += ");";

            PGresult* res = PQexec(con, query.c_str());

            if (PQresultStatus(res) != PGRES_COMMAND_OK) {
                cerr << "Insert failed: " << PQerrorMessage(con) << endl;
                PQclear(res);
                PQfinish(con);
                return 1;
            }
            PQclear(res);
        }
    }
    catch (const exception& e) {
        cerr << "Error reading CSV: " << e.what() << endl;
        PQfinish(con);
        return 1;
    }

    PQfinish(con);
    cout << "Done and connection closed." << endl;

    return 0;
}