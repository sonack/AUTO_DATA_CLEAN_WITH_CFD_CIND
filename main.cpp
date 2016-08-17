#include <string>
#include <vector>
#include <iostream>
#include <fstream>
#include <sstream>
#include <cstdlib>
#include <cctype>
#include <iomanip>
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
        Tableau(const char *name):name(name),rows(0){}
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
                cout << schema[i] << "\t";
            cout << endl;
            for(int i=0;i<rows;i++)
            {
                for(int j=0;j<fields;j++)
                    cout << data[i][j] << "\t";
                cout << endl;
            }
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
    exp = "(" + a + " = " + b + " OR " + b + " = " + "'_')";
    return exp;
}
string defineNotEqual(string a,string b)
{
    string exp;
    exp = "( NOT " + defineEqual(a,b) + " )";
    return exp;
}

/**

构建插入模式表的SQL语句
*/
string buildPatternRow(int tableauNo)
{
    string sql = "INSERT INTO " + patternTableName;
    sql += "( ";
    Tableau &Tp = *T[tableauNo];

    for(int i = 0,f = Tp.getFields(); i < f; i++)
    {
        if(i)   sql += " , ";
        sql += Tp.getSchema(i);
    }

    sql += ") VALUES ";

    for(int i = 0, r = Tp.getRows(); i < r; i++)
    {
        if(i)   sql += " , ";
        sql += "( ";
        for(int j = 0, f = Tp.getFields(); j < f; j++)
        {
            if(j) sql += " , ";
            sql += "'" + Tp.getData(i,j) + "'";
        }
        sql += " )";
    }

    sql += ";";
    return sql;
}

/**
执行构建模式表的SQL语句
*/
void buildPatternTable()
{
    for(int i=0;i<tableauNum;i++)
    {
        mysql_query(&mysql,buildPatternRow(i).c_str());
        T[i]->display();
    }
    cout << "构建规则表成功!(共" << tableauNum << "张)" << endl;
    cout << "==============================================" << endl << endl;

}

/**

构建常CFD查询语句
*/
string buildConstantQuery(int tableauNo)
{
    string sql = "SELECT t.* FROM ";
    sql = sql + dataTableName;
    sql = sql + " as t, ";
    sql = sql + patternTableName;
    sql = sql + " as tp";
    sql = sql + " WHERE ";
    Tableau &Tp = *T[tableauNo];
    int i = 0;
    for(int d = Tp.getDelimiter(); i < d; i++)
    {
        sql = sql + defineEqual("t." + Tp.getSchema(i),"tp." + Tp.getSchema(i)) + " AND ";
    }
    sql += "( ";
    bool first = true;
    for(int f = Tp.getFields(); i < f; i++)
    {
        if(!first) sql = sql + " OR ";
        sql = sql + defineNotEqual("t." + Tp.getSchema(i),"tp." + Tp.getSchema(i));
        if(first) first = false;
    }
    sql += " );";
    return sql;
}

/**

展现查询结果 res
*/
void displayResult(int tableauNum)
{
    int rowCnt = mysql_num_rows(res);
    int fieldCnt = mysql_num_fields(res);
    for(int i=0;i<fieldCnt;i++)
    {
        field = mysql_fetch_field_direct(res,i);
        cout << field->name << "\t";
    }
    cout << endl;
    MYSQL_ROW row = NULL;
    row = mysql_fetch_row(res);
    while(row != NULL)
    {
        for(int i=0;i<fieldCnt;i++)
            cout << row[i] <<"\t\t";
        cout << endl;
        row = mysql_fetch_row(res);
    }
    cout << "共有" << rowCnt << "条违例." << endl;

}
void ConstantQuery()
{
    for(int i=0;i<tableauNum;i++)
    {
        mysql_query(&mysql,buildConstantQuery(i).c_str());
        res = mysql_store_result(&mysql);
        cout << T[i]->getName() << " Qc违例如下:" << endl;
        displayResult(i);
    }

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
    mysql_query(&mysql,"delete from T");
}
int main()
{
   // freopen("out.txt","w",stdout);
    getRules();         //从ruleFile文件中读取特定格式描述的CFD规则
    initMySQL();        //初始化MySQL连接
    buildPatternTable();    //根据读取的CFD规则在MySQL中构建模式表Tableau
    ConstantQuery();    //进行常CFD查询(aka: single constant)
    closeMySQL();   //关闭MySQL连接
    return 0;
}
