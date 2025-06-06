﻿using System.IO;
using Cave.Data;
using NUnit.Framework;

namespace Test.Cave.Data
{
    [TestFixture]
    public class DatReaderWriter
    {
        #region Public Methods

        [Test]
        public void StructReadWrite()
        {
            var stream = new MemoryStream();
            var writer = new DatWriter(RowLayout.CreateTyped(typeof(TestStructClean)), stream);
            for (var i = 0; i < 100; i++)
            {
                var t = TestStructClean.Create(i);
                writer.Write(t);
            }

            stream.Seek(0, SeekOrigin.Begin);
            var reader = new DatReader(stream);
            for (var i = 0; i < 100; i++)
            {
                Assert.IsTrue(reader.ReadRow<TestStructClean>(true, out var t));
                Assert.AreEqual(t, TestStructClean.Create(i));
            }
        }

        #endregion Public Methods
    }
}
