using System.Collections.Generic;
using System.Linq;
using BenchmarkDotNet.Attributes;
using Cave.Data;
using Microsoft.VSDiagnostics;

namespace Cave.Data.Benchmarks;

[CPUUsageDiagnoser]
public class MemoryStorageCloneBenchmark
{
    const int TotalRowCount = 50_000;
    const int DatabaseCount = 2;
    const int TablesPerDatabase = 2;
    const int RowCountPerTable = TotalRowCount / (DatabaseCount * TablesPerDatabase);

    IStorage populatedStorage = null!;

    [GlobalSetup]
    public void Setup() => populatedStorage = CreatePopulatedStorage();

    static IStorage CreatePopulatedStorage()
    {
        var storage = new MemoryStorage();
        var id = 1;
        for (var d = 0; d < DatabaseCount; d++)
        {
            var database = storage.CreateDatabase($"db{d}");
            for (var t = 0; t < TablesPerDatabase; t++)
            {
                var layout = RowLayout.CreateTyped(typeof(BenchStruct), $"table{t}");
                var table = database.CreateTable(layout);
                var rows = new List<Row>(RowCountPerTable);
                for (var i = 0; i < RowCountPerTable; i++)
                {
                    rows.Add(layout.GetRow(BenchStruct.Create(id++)));
                }

                table.Insert(rows);
            }
        }

        return storage;
    }

    /// <summary>Benchmarks creation of 50.000 records distributed over multiple <see cref="MemoryTable"/>s in two <see cref="MemoryDatabase"/>s inside one <see cref="MemoryStorage"/>.</summary>
    [Benchmark]
    public IStorage CreateStorageWithRows() => CreatePopulatedStorage();

    /// <summary>Benchmarks cloning of a populated <see cref="MemoryStorage"/> into a new one.</summary>
    [Benchmark]
    public IStorage CloneStorage()
    {
        var readonlyInstance = populatedStorage;
        var result = new MemoryStorage();
        foreach (var dbname in readonlyInstance.DatabaseNames)
        {
            var sourcedb = readonlyInstance.GetDatabase(dbname);
            var targetdb = result.CreateDatabase(dbname);
            foreach (var tablename in sourcedb.TableNames)
            {
                var sourcetable = sourcedb.GetTable(tablename);
                var targettable = targetdb.CreateTable(sourcetable.Layout);

                if (sourcetable is MemoryTable sourcemem && targettable is MemoryTable targetmem)
                {
                    sourcemem.CopyTo(targetmem);
                }
                else
                {
                    targettable.Insert(sourcetable.GetRows());
                }
            }
        }

        return result;
    }
}
