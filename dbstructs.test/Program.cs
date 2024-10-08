using Cave.Data;

namespace dbstructs.test;

class Program
{
    #region Private Methods

    static int Main(string[] args)
    {
        var dir = Directory.GetCurrentDirectory();
        while (!File.Exists(dir + "/dbstructs.test.csproj"))
        {
            dir = Path.GetFullPath(dir + "/..");
            if (dir.EndsWith(".."))
            {
                Console.WriteLine("Could not find dbstructs.test path...");
                return 1;
            }
        }
        dir = Path.GetFullPath(dir + "/Test");
        Directory.CreateDirectory(dir);
        Console.WriteLine($"Create test db structures at {dir}");

        var db = MemoryDatabase.Default;
        db.CreateTable<SampleRow>("sample");
        {
            var interfaceV1 = db.GenerateInterface(nameSpace: "dbstructs.test", className: "SampleV1");
            interfaceV1.Options.OutputDirectory = dir;
            interfaceV1.Options.DisableKnownIdentifiers = true;
            interfaceV1.Options.IdentifierHashCode = true;
            foreach (var table in db)
            {
                var result = interfaceV1.GenerateTableStructFile(table);
                Console.WriteLine($"+ {result.FileName}");
            }
            interfaceV1.Save();
            Console.WriteLine($"+ {interfaceV1.FileName}");
        }

        {
            var interfaceV2 = db.GenerateInterface(nameSpace: "dbstructs.test", className: "SampleV2");
            interfaceV2.Options.OutputDirectory = dir;
            foreach (var table in db)
            {
                var result = interfaceV2.GenerateTableStructFile(table);
                Console.WriteLine($"+ {result.FileName}");
            }
            interfaceV2.Save();
            Console.WriteLine($"+ {interfaceV2.FileName}");
        }

        return 0;
    }

    #endregion Private Methods
}
