using Cave.Data;

namespace dbtest;

class Program
{
    #region Private Methods

    static void DropTables(IDatabase database)
    {
        foreach (var table in database)
        {
            database.DeleteTable(table.Name);
        }
    }

    static int Main(string[] args)
    {
        var db = Connector.ConnectDatabase("mysql://test:test@localhost/test", ConnectionFlags.AllowCreate);
        DropTables(db);
        var table = db.CreateTable<TestRow>();

        for (var i = 0; i < 1000; i++)
        {
            Console.Write(".");
            var test1 = TestRow.Create();
            var test2 = table.Insert(test1);
            if (table.Exist(test1)) throw new Exception("Should not exist");
            if (!table.Exist(test2)) throw new Exception("Should exist");
        }

        return 0;
    }

    #endregion Private Methods
}
