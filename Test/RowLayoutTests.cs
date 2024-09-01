using System;
using System.Collections.Generic;
using System.Linq;
using Cave;
using Cave.Data;
using NUnit.Framework;

namespace Test.Cave.Data
{
    [TestFixture]
    public class RowLayoutTests
    {
        #region Private Methods

        static void CreateField(ref List<IFieldProperties> fields, FieldFlags flags, DataType dataType, string name, Type valueType = null)
        {
            var field = new FieldProperties
            {
                Index = fields.Count,
                Name = name,
                Flags = flags,
                DataType = dataType,
                ValueType = valueType
            }.Validate();
            fields.Add(field);
        }

        #endregion Private Methods

        #region Public Methods

        [Test]
        public void CheckLayout()
        {
            var layoutA = RowLayout.CreateTyped(typeof(TestStructBug));
            var fields = new List<IFieldProperties>();
            CreateField(ref fields, FieldFlags.ID, DataType.UInt64, "IDField");
            CreateField(ref fields, FieldFlags.Index, DataType.UInt32, "IndexedField");
            CreateField(ref fields, FieldFlags.Unique, DataType.Int16, "UniqueField");
            CreateField(ref fields, FieldFlags.AutoIncrement, DataType.UInt16, "AutoIncField");
            CreateField(ref fields, FieldFlags.Index | FieldFlags.AutoIncrement, DataType.UInt8, "AutoIncIndexField");
            CreateField(ref fields, FieldFlags.Unique | FieldFlags.Index, DataType.Int8, "UniqueIndexedField");
            CreateField(ref fields, FieldFlags.Index | FieldFlags.AutoIncrement | FieldFlags.Unique, DataType.Int64, "AutoIncUniqueIndexedField");
            CreateField(ref fields, FieldFlags.None, DataType.Enum, "SomeEnum", typeof(Environment.SpecialFolder));
            CreateField(ref fields, FieldFlags.None, DataType.String, "BuggyField");
            var layoutB = RowLayout.CreateUntyped("TestStruct", fields.ToArray());
            RowLayout.CheckLayout(layoutB, layoutA);
        }

        [Test]
        public void MissingField()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));

            var dbWithMissingField = RowLayout.CreateUntyped("TableName", layout.Fields.Where(f => !f.Name.EndsWith("B")).ToArray());

            var store = new MemoryStorage();
            var db = store.CreateDatabase("db");
            var table = db.CreateTable(dbWithMissingField, TableFlags.IgnoreMissingFields);

            var typedTable = new Table<TestStructClean>(table);

            var list = new List<TestStructClean>();
            for (int i = 0; i < 10; i++)
            {
                var row = TestStructClean.Create(i);
                var insertedRow = typedTable.Insert(row);
                row.ID = insertedRow.ID;
                list.Add(row);

                row.B = default;
                row.SB = default;
                Assert.AreEqual(row, insertedRow);
            }

            foreach (var row in list)
            {
                var rowWithMissingFields = row;
                rowWithMissingFields.B = default;
                rowWithMissingFields.SB = default;

                var dbRow = typedTable.GetStruct(row.ID);
                Assert.AreEqual(rowWithMissingFields, dbRow);
            }
        }

        [Test]
        public void TypedCheck()
        {
            var layout = RowLayout.CreateTyped(typeof(TestStructClean));
            foreach (var field in layout.Fields)
            {
                Assert.AreEqual(field.Name, field.NameAtDatabase);
            }
        }

        #endregion Public Methods
    }
}
