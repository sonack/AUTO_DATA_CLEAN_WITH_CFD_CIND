#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <cctype>
#include <iomanip>
#include <algorithm>
#include <mysql/mysql.h>

using namespace std;
const char ruleFile[] = "rules.dat";
const char dataBase[] = "DataCleaning";
string dataTableName = "employee";
string patternTableName = "T";
const int maxTableau = 10,
           maxRow     = 10,
           maxField   = 20;

int tableauNum;

MYSQL mysql;
MYSQL_RES *res;
MYSQL_FIELD *field;

vector<string> Z,W;

/**
pattern tableau 对象
一张表对应一个对象
*/
class Tableau
{
    private:
        string name;                //表名
        string schema[maxField];    //模式名(首行)
        string data[maxRow][maxField];  //各条数据记录
        int rows,fields,delimiter;  //行数\域数\分隔符位置
    public:
        Tableau(){}
        Tableau(const char *name):name(name),rows(0),delimiter(-1){}
        void buildSchema(string l)
        {
            istringstream iss(l);
            string word;
            int cur = 0;
            for(int pos = 0; iss >> word; ++pos)
            {
                if(word == "|")
                    {
                        delimiter = pos;
                        continue;
                    }
                schema[cur++] = word;
                if(delimiter == -1) Z.push_back(word);
                else W.push_back(word);
            }
            fields = cur;
        }

        void buildRow(string l)
        {
            istringstream iss(l);
            string word;
            for(int pos = 0; iss >> word; ++pos)
            {
                data[rows][pos] = word;
            }
            rows++;
        }

        void display()
        {
            cout << getName() << endl;
            for(int i=0;i<fields;i++)
            {
                if(i == delimiter) cout << "|\t";
                cout << schema[i] << "\t";
            }
            cout << endl;
            for(int i=0;i<rows;i++)
            {
                for(int j=0;j<fields;j++)
                {
                    if(j == delimiter) cout << "|\t";
                    cout << data[i][j] << "\t";
                }
                cout << endl;
            }
            cout << endl;
        }

        string getName()
        {
            return "模式表:" + name;
        }

        int getRows()
        {
            return rows;
        }

        int getFields()
        {
            return fields;
        }

        int getDelimiter()
        {
            return delimiter;
        }

        string getSchema(int idx)
        {
            return schema[idx];
        }
        string getData(int row,int field)
        {
            return data[row][field];
        }

}* T[maxTableau];


/**
构建表Z和W的SQL语句并执行
*/
void getTableauSchema(vector<string> &v,string tableName)
{

    sort(v.begin(),v.end());
    vector<string>::iterator iter = unique(v.begin(),v.end());
    if(iter != v.end())
        v.erase(iter,v.end());

    string sql = "CREATE TABLE " + tableName + "( id INT  NOT NULL, ";
    for(unsigned int i=0; i < v.size(); i++)
        sql += v[i] + " VARCHAR(30) DEFAULT '@' , ";
    sql += "PRIMARY KEY(id));";
    mysql_query(&mysql,sql.c_str());

}


/**
读取文件并构建Tableau对象
*/
void getRules()
{
    ifstream rules(ruleFile,ios::in);
    if(!rules)
    {
        cerr << "Open RuleFile Fail!" << endl;
        exit(1);
    }
    string line;
    int cur = -1;
    while(getline(rules,line))
    {
        if(line[0] == '#')
        {
            cur++;
            T[cur] =  new Tableau(&line[1]);
            getline(rules,line);
            T[cur]->buildSchema(line);
        }
        else if(line.length() > 0 && !isspace(line[0]))
        {
            T[cur]->buildRow(line);
        }
    }
    tableauNum = cur + 1;
}


/**
构建基于'_'的判等语句
*/
string defineEqual(string a,string b)
{
    string exp;
    exp = "(" + a + " = " + b + " OR " + b + " = " + "'_' OR " + b + " = " + "'@')";
    return exp;
}
string defineNotEqual(string a,string b)
{
    string exp;
    exp = "( NOT " + defineEqual(a,b) + " )";
    return exp;
}

string num2str(int i)
{
    string str;
    ostringstream oss;
    oss << i ;
    return oss.str();
}

/**

将一个CFD的Tableau合并
*/
void buildPatternTable(int tableauNo)
{
    static int id = 1;
    string sql1 = "INSERT INTO Z ( id", sql2 = "INSERT INTO W ( id";

    Tableau &Tp = *T[tableauNo];

    int i = 0;
    for(int d = Tp.getDelimiter(); i < d; i++)
    {
        sql1 += " , " + Tp.getSchema(i);
    }
    for(int f = Tp.getFields(); i < f; i++)
    {
        sql2 += " , " + Tp.getSchema(i);
    }

    sql1 += ") VALUES ";
    sql2 += ") VALUES ";
    for(int i = 0, r = Tp.getRows(); i < r; i++)
    {
        if(i)
        {
            sql1 += " , ";
            sql2 += " , ";
        }
        sql1 += "( '" + num2str(id) + "' , ";
        sql2 += "( '" + num2str(id++) + "' , ";
        int j = 0;
        for(int d = Tp.getDelimiter(); j < d; j++)
        {
            if(j) sql1 += " , ";
            sql1 += "'" + Tp.getData(i,j) + "'";
        }
        sql1 += " )";
        bool first = true;
        for(int f = Tp.getFields(); j < f; j++)
        {
            if(first) first = false;
            else sql2 += " , ";
            sql2 += "'" + Tp.getData(i,j) + "'";
        }
        sql2 += " )";
    }

    sql1 += ";";
    sql2 += ";";
//    cout << "SQL1 : " << sql1 << endl;
//    cout << "SQL2 : " << sql2 << endl;
    mysql_query(&mysql,sql1.c_str());
    mysql_query(&mysql,sql2.c_str());
}

/**
执行构建模式表的SQL语句
*/
void buildPatternAllTable()
{
    for(int i=0;i<tableauNum;i++)
    {
        buildPatternTable(i);
        T[i]->display();
    }
    cout << "构建、合并模式表成功!(共合并" << tableauNum << "张)" << endl;
    cout << "==============================================" << endl << endl;

}

/**
构建常CFD查询语句
*/
string buildConstantQuery()
{
    string sql = "SELECT t.* FROM ";
    sql = sql + dataTableName;
    sql = sql + " AS t, ";
    sql = sql + "Z AS tpz, ";
    sql = sql + "W AS tpw";
    sql = sql + " WHERE tpz.id = tpw.id AND ";

    for(unsigned int i = 0; i < Z.size(); i++)
    {
        sql = sql + defineEqual("t." + Z[i],"tpz." + Z[i]) + " AND ";
    }
    sql += "( ";
    bool first = true;
    for(unsigned int i = 0; i < W.size(); i++)
    {
        if(first) first = false;
        else sql = sql + " OR ";
        sql = sql + defineNotEqual("t." + W[i],"tpw." + W[i]);

    }
    sql += " );";
    return sql;
}



/**
构建变CFD查询语句
*/

string buildVariableQuery(int tableauNo)
{
    Tableau &Tp = *T[tableauNo];
    string sql = "SELECT GROUP_CONCAT( DISTINCT ";
    for(int i = 0, d = Tp.getDelimiter(); i < d; i++)
    {
        if(i) sql += " , ',' , ";        // 分隔
        sql += "t." + Tp.getSchema(i);
    }
    sql += " ) violation_X FROM ";  // 命名为violation_X
    sql = sql + dataTableName;
    sql = sql + " as t, ";
    sql = sql + patternTableName;
    sql = sql + " as tp";
    sql = sql + " WHERE ";
    int i = 0;
    for(int d = Tp.getDelimiter(); i < d; i++)
    {
       sql += defineEqual("t." + Tp.getSchema(i),"tp." + Tp.getSchema(i)) + " AND ";
    }
    sql += "( ";
    bool first = true;
    for(int f = Tp.getFields(); i < f; i++)
    {
        if(first) first = false;
        else sql = sql + " OR ";
        sql = sql + "tp." + Tp.getSchema(i) + " = '_'";

    }
    sql += " ) GROUP BY ";
    i = 0;
    for(int d = Tp.getDelimiter(); i < d; i++)
    {
       if(i) sql += " , ";
       sql += "t." + Tp.getSchema(i);
    }
    sql += " HAVING COUNT( DISTINCT ";
    first = true;
    for(int f = Tp.getFields(); i < f; i++)
    {
        if(first) first = false;
        else sql = sql + " , ";
        sql = sql + "t." + Tp.getSchema(i);

    }
    sql += " ) > 1;";
    return sql;
}
/**

展现查询结果 res
*/
void displayResult()
{
    int rowCnt = mysql_num_rows(res);
    int fieldCnt = mysql_num_fields(res);
    for(int i=0;i<fieldCnt;i++)
    {
        field = mysql_fetch_field_direct(res,i);
        cout << field->name << "  ";
    }
    cout << endl;
    MYSQL_ROW row = NULL;
    row = mysql_fetch_row(res);
    while(row != NULL)
    {
        for(int i=0;i<fieldCnt;i++)
            cout << row[i] <<"  ";
        cout << endl;
        row = mysql_fetch_row(res);
    }
    cout << "共有" << rowCnt << "条违例." << endl;

}

void VariableQuery()
{
    for(int i=0;i<tableauNum;i++)
    {
        mysql_query(&mysql,buildVariableQuery(i).c_str());
        res = mysql_store_result(&mysql);
        cout << T[i]->getName() << " Qv违例如下:" << endl;
        displayResult();
    }
}
void ConstantQuery()
{
    mysql_query(&mysql,buildConstantQuery().c_str());
    res = mysql_store_result(&mysql);
    cout << "Qc违例如下:" << endl;
    displayResult();
}

void closeMySQL()
{
    mysql_close(&mysql);
}
void initMySQL()
{
    mysql_init(&mysql);
    mysql_real_connect(&mysql,"localhost","root","liubixue",dataBase,3306,NULL,0);
    mysql_set_character_set(&mysql,"utf8");
    mysql_query(&mysql,"drop table T");
    mysql_query(&mysql,"drop table Z");
    mysql_query(&mysql,"drop table W");
}

void Init()
{
    Z.clear();
    W.clear();
    getRules();         //从ruleFile文件中读取特定格式描述的CFD规则
    initMySQL();        //初始化MySQL连接
    getTableauSchema(Z,"Z");
    getTableauSchema(W,"W");

}
int main()
{
   // freopen("out.txt","w",stdout);
    Init();
    buildPatternAllTable();    //根据读取的CFD规则在MySQL中构建模式表Tableau
    ConstantQuery();    //进行常CFD查询(aka: single constant)
    VariableQuery();    //进行变CFD查询(aka: multiple constant)





    closeMySQL();   //关闭MySQL连接
    return 0;
}
