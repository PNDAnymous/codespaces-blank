#include <iostream>
#include <fstream>
#include <regex>
#include "csv.hpp"
#include <libpq-fe.h>

using namespace std;
using namespace csv;

string trim(const string &s)
{
    size_t start = s.find_first_not_of(" \t\n\r");
    if (start == string::npos)
    {
        return "";
    }

    size_t end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

string cleanString(const string &input)
{
    string s = trim(input);
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"')
    {
        s = s.substr(1, s.size() - 2);
    }
    return s;
}

string build_con_string()
{
    const char *host = getenv("PGHost");
    const char *port = getenv("PGPort");
    const char *dbName = getenv("PGDBName");
    const char *user = getenv("PGUser");
    const char *password = getenv("PGPW");
    const char *sslMode = getenv("PGSSLMODE");

    if (!host || !port || !dbName || !user || !password)
    {
        cerr << "Missing DB environment variables" << endl;
        exit(1);
    }

    string conStr = "host=" + string(host) +
                    " port=" + string(port) +
                    " dbname=" + string(dbName) +
                    " user=" + string(user) +
                    " password=" + string(password);

    if (sslMode)
    {
        conStr += " sslmode=" + string(sslMode);
    }
    return conStr;
}

string escapeSQLString(const string &input)
{
    string output;
    for (char c : input)
    {
        if (c == '\'')
        {
            output += "''";
        }
        else
        {
            output += c;
        }
    }
    return output;
}

enum class colType
{
    Boolean,
    Int,
    Float,
    Date,
    Text
};
colType infer_type(const vector<string> &samples)
{
    bool all_int = true, all_float = true, all_bool = true, all_date = true;

    for (const auto &val : samples)
    {
        if (val.empty())
            continue;

        if (!regex_match(val, regex("^-?\\d+$")))
            all_int = false;
        if (!regex_match(val, regex("^-?\\d*\\.\\d+$")))
            all_float = false;
        if (!regex_match(val, regex("^(true|false|yes|no|0|1)$", regex::icase)))
            all_bool = false;
        if (!regex_match(val, regex("^\\d{4}-\\d{2}-\\d{2}$")))
            all_date = false;
    }

    if (all_bool)
        return colType::Boolean;
    if (all_int)
        return colType::Int;
    if (all_float)
        return colType::Float;
    if (all_date)
        return colType::Date;

    return colType::Text;
}

string colTypeToPG(colType t)
{
    switch (t)
    {
    case colType::Boolean:
        return "BOOLEAN";
    case colType::Int:
        return "INTEGER";
    case colType::Float:
        return "FLOAT";
    case colType::Date:
        return "DATE";
    default:
        return "TEXT";
    }
}

int main()
{

    string conInfo = build_con_string();
    PGconn *con = PQconnectdb(conInfo.c_str());

    if (PQstatus(con) != CONNECTION_OK)
    {
        cerr << "Connection to database failed: " << PQerrorMessage(con);
        PQfinish(con);
        return 1;
    }
    cout << "Connected to Database" << endl;

    try
    {
        CSVReader reader("nursery_data.csv");
        vector<string> headers = reader.get_col_names();

        if (headers.empty())
        {
            cout << "No headers found, DEFAULT=COL1,COL2,ETC." << endl;
            CSVRow firstRow;
            if (reader.begin() != reader.end())
            {
                firstRow = *reader.begin();
            }
            else
            {
                cerr << "Empty CSV file!" << endl;
                PQfinish(con);
                return 1;
            }
            headers.clear();
            for (size_t i = 0; i < firstRow.size(); i++)
            {
                headers.push_back("COL" + to_string(i + 1));
            }
        }
        else
        {
            cout << "Headers detected" << endl;
            for (auto &name : headers)
            {
                cout << " - " << name << endl;
            }
        }

        // Sample -> Type
        vector<vector<string>> samples(headers.size());
        int sampleLimit = 100;
        int sampleCount = 0;
        for (CSVRow &row : reader)
        {
            for (size_t i = 0; i < headers.size(); i++)
            {
                if (i < row.size())
                {
                    samples[i].push_back(cleanString(row[i].get<string>()));
                }
            }
            if (++sampleCount >= sampleLimit)
                break;
        }

        vector<colType> colTypes;
        for (auto &colSamples : samples)
        {
            colTypes.push_back(infer_type(colSamples));
        }

        string theQuerier = "DROP TABLE IF EXISTS nursery_db;\nCREATE TABLE nursery_db (id SERIAL PRIMARY KEY, ";
        for (size_t i = 0; i < headers.size(); ++i)
        {
            theQuerier += "\"" + headers[i] + "\" " + colTypeToPG(colTypes[i]);
            if (i < headers.size() - 1)
                theQuerier += ", ";
        }
        theQuerier += ");";

        cout << "Creating Table with The Querier:\n"
             << theQuerier << endl;
        PGresult *res = PQexec(con, theQuerier.c_str());
        if (PQresultStatus(res) != PGRES_COMMAND_OK)
        {
            cerr << "Creating table failure: " << PQerrorMessage(con) << endl;
            PQclear(res);
            PQfinish(con);
            return 1;
        }
        PQclear(res);

        CSVReader readerDos("nursery_data.csv");
        PQexec(con, "BEGIN;");
        for (CSVRow &row : readerDos)
        {
            string query = "INSERT INTO nursery_db (";
            for (size_t i = 0; i < headers.size(); ++i)
            {
                query += "\"" + headers[i] + "\"";
                if (i < headers.size() - 1)
                    query += ", ";
            }
            query += ") VALUES(";
            for (size_t i = 0; i < headers.size(); ++i)
            {
                string val = (i < row.size()) ? cleanString(row[i].get<string>()) : "";
                string escaped = escapeSQLString(val);
                if (val.empty())
                {
                    query += "NULL";
                }
                else if (colTypes[i] == colType::Text || colTypes[i] == colType::Date || colTypes[i] == colType::Boolean)
                {
                    query += "'" + escapeSQLString(val) + "'";
                }
                else
                {
                    query += escapeSQLString(val);
                }
                if (i < headers.size() - 1)
                    query += ", ";
            }
            query += ");";

            PGresult *res = PQexec(con, query.c_str());
            if (PQresultStatus(res) != PGRES_COMMAND_OK)
            {
                cerr << "Insert Failed: " << PQerrorMessage(con) << endl;
                PQclear(res);
                PQfinish(con);
                return 1;
            }
            PQclear(res);
        }
        PQexec(con, "COMMIT;");
    }
    catch (const exception &e)
    {
        cerr << "Error reading CSV: " << e.what() << endl;
        PQfinish(con);
        return 1;
    }

    PQfinish(con);
    cout << "Done and connection closed." << endl;

    return 0;
}