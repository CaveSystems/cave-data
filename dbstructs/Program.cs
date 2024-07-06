using Cave;
using Cave.Data;
using MySqlConnector;

namespace dbstructs;

class Program
{
    #region Private Methods

    static void Main(string[] args)
    {
        Console.WriteLine("Create database structures");

        if (!args.Any())
        {
            Console.WriteLine("Usage:\ndbstructs <connectionstring> [parameter1 [parameter2 [..]]]");
            return;
        }
        try
        {
            using var mysql = new MySqlConnection();
            ConnectionString conStr = args.FirstOrDefault();
            Console.WriteLine($"Connecting to {conStr.ToString(ConnectionStringPart.NoCredentials)}");
            var con = Connector.ConnectStorage(conStr, ConnectionFlags.AllowUnsafeConnections);

            foreach (var dbname in con.DatabaseNames)
            {
                if (dbname == "mysql") continue;
                if (dbname == "information_schema") continue;
                if (dbname == "performance_schema") continue;
                if (dbname == "sys") continue;

                Console.WriteLine($"Database {dbname}");
                var db = con[dbname];
                var generatedInterface = db.GenerateInterface();
                foreach (var tabname in db.TableNames)
                {
                    Console.WriteLine($"+ {tabname}");
                    var table = db[tabname];
                    var result = generatedInterface.GenerateTableStructFile(table);
                }
                generatedInterface.Save();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.ToString());
        }
    }

    #endregion Private Methods
}
