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
            Console.WriteLine("Usage: dbstructs <connectionstring> [--option [--option2 [..]]]");
            Console.WriteLine("");
            Console.WriteLine("Options:");
            Console.WriteLine("  --version=1 [/-v1] writes v1 structures without simple key/identifier accessors.");
            return;
        }
        try
        {
            using var mysql = new MySqlConnection();
            ConnectionString conStr = args.FirstOrDefault() ?? string.Empty;
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
                generatedInterface.Options.DisableKnownIdentifiers = args.Any(a => a == "--version=1" || a == "-v1");
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
