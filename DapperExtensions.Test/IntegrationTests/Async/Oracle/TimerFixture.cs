using DapperExtensions.Test.Data.Oracle;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using Car = DapperExtensions.Test.Data.Common.Car;

namespace DapperExtensions.Test.IntegrationTests.Async.Oracle
{
    [TestFixture]
    [Parallelizable(ParallelScope.All)]
    public static class TimerFixture
    {
        private const int cnt = 1000;

        public class InsertTimes : OracleBaseAsyncFixture
        {
            [Test]
            public void IdentityKey_UsingEntity()
            {
                var p = new Person
                {
                    FirstName = "FirstName",
                    LastName = "LastName",
                    DateCreated = DateTime.Now,
                    Active = "Y"
                };
                Db.InsertAsync(p);
                var start = DateTime.Now;
                var ids = new List<long>();
                for (var i = 0; i < cnt; i++)
                {
                    var p2 = new Person
                    {
                        FirstName = "FirstName" + i,
                        LastName = "LastName" + i,
                        DateCreated = DateTime.Now,
                        Active = "Y"
                    };
                    Db.InsertAsync(p2);
                    ids.Add(p2.Id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + (total / cnt));
                Dispose();
            }

            [Test]
            public void IdentityKey_UsingReturnValue()
            {
                var p = new Person
                {
                    FirstName = "FirstName",
                    LastName = "LastName",
                    DateCreated = DateTime.Now,
                    Active = "Y"
                };
                Db.InsertAsync(p);
                var start = DateTime.Now;
                var ids = new List<long>();
                for (var i = 0; i < cnt; i++)
                {
                    var p2 = new Person
                    {
                        FirstName = "FirstName" + i,
                        LastName = "LastName" + i,
                        DateCreated = DateTime.Now,
                        Active = "Y"
                    };
                    var id = Db.InsertAsync(p2).Result;
                    ids.Add(id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + (total / cnt));
                Dispose();
            }

            [Test]
            [Ignore("Oracle does not support GUID from the box")]
            public void GuidKey_UsingEntity()
            {
                var a = new Animal { Name = "Name" };
                Db.InsertAsync(a);
                var start = DateTime.Now;
                //var ids = new List<Guid>();
                for (var i = 0; i < cnt; i++)
                {
                    var a2 = new Animal { Name = "Name" + i };
                    Db.InsertAsync(a2);
                    //ids.Add(a2.Id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + total / cnt);
                Dispose();
            }

            [Test]
            [Ignore("Oracle does not support GUID from the box")]
            public void GuidKey_UsingReturnValue()
            {
                var a = new Animal { Name = "Name" };
                Db.InsertAsync(a);
                var start = DateTime.Now;
                var ids = new List<Guid>();
                for (var i = 0; i < cnt; i++)
                {
                    var a2 = new Animal { Name = "Name" + i };
                    var id = Db.InsertAsync(a2).Result;
                    ids.Add(id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + (total / cnt));
                Dispose();
            }

            [Test]
            public void AssignKey_UsingEntity()
            {
                var ca = new Car { Id = string.Empty.PadLeft(15, '0'), Name = "Name" };
                Db.InsertAsync(ca);
                var start = DateTime.Now;
                var ids = new List<string>();
                for (var i = 0; i < cnt; i++)
                {
                    var key = (i + 1).ToString().PadLeft(15, '0');
                    var ca2 = new Car { Id = key, Name = "Name" + i };
                    Db.InsertAsync(ca2);
                    ids.Add(ca2.Id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + (total / cnt));
                Dispose();
            }

            [Test]
            public void AssignKey_UsingReturnValue()
            {
                var ca = new Car { Id = string.Empty.PadLeft(15, '0'), Name = "Name" };
                Db.InsertAsync(ca);
                var start = DateTime.Now;
                var ids = new List<string>();
                for (var i = 0; i < cnt; i++)
                {
                    var key = (i + 1).ToString().PadLeft(15, '0');
                    var ca2 = new Car { Id = key, Name = "Name" + i };
                    var id = Db.InsertAsync(ca2).Result;
                    ids.Add(id);
                }

                var total = DateTime.Now.Subtract(start).TotalMilliseconds;
                Console.WriteLine("Total Time:" + total);
                Console.WriteLine("Average Time:" + (total / cnt));
                Dispose();
            }
        }
    }
}