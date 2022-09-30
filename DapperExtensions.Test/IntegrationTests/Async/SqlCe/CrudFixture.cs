﻿#if NETFRAMEWORK
using DapperExtensions.Predicate;
using DapperExtensions.Test.Data.Common;
using FluentAssertions;
using NUnit.Framework;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DapperExtensions.Test.IntegrationTests.Async.SqlCe
{
    [TestFixture]
    [Parallelizable(ParallelScope.Self)]
    public static class CrudFixture
    {
        [TestFixture]
        public class InsertMethod : SqlCeBaseFixture
        {
            [Test]
            public void AddsEntityToDatabase_ReturnsKey()
            {
                var p = new Person { Active = true, FirstName = "Foo", LastName = "Bar", DateCreated = DateTime.UtcNow };
                var id = Db.InsertAsync(p).Result;
                Assert.AreEqual(1, id);
                Assert.AreEqual(1, p.Id);
                Dispose();
            }

            [Test]
            public void AddsEntityToDatabase_ReturnsCompositeKey()
            {
                var m = new Multikey { Key2 = "key", Value = "foo" };
                var key = Db.InsertAsync(m).Result;
                Assert.AreEqual(1, key.Key1);
                Assert.AreEqual("key", key.Key2);
                Dispose();
            }

            [Test]
            public void AddsEntityToDatabase_ReturnsGeneratedPrimaryKey()
            {
                var a1 = new Animal { Name = "Foo" };
                Db.InsertAsync(a1);

                var a2 = Db.GetAsync<Animal>(a1.Id).Result;
                Assert.AreNotEqual(Guid.Empty, a2.Id);
                Assert.AreEqual(a1.Id, a2.Id);
                Dispose();
            }

            [Test]
            public void AddsEntityToDatabase_WithPassedInGuid()
            {
                var guid = Guid.NewGuid();
                var a1 = new Animal { Id = guid, Name = "Foo" };
                Db.InsertAsync(a1);

                var a2 = Db.GetAsync<Animal>(a1.Id).Result;
                Assert.AreNotEqual(Guid.Empty, a2.Id);
                Assert.AreEqual(guid, a2.Id);
                Dispose();
            }

            [Test]
            public void AddsMultipleEntitiesToDatabase()
            {
                var a1 = new Animal { Name = "Foo" };
                var a2 = new Animal { Name = "Bar" };
                var a3 = new Animal { Name = "Baz" };

                Db.InsertAsync<Animal>(new[] { a1, a2, a3 });

                var animals = Db.GetListAsync<Animal>().Result.ToList();
                Assert.AreEqual(3, animals.Count);
                Dispose();
            }

            [Test]
            public void AddsMultipleEntitiesToDatabase_WithPassedInGuid()
            {
                var guid1 = Guid.NewGuid();
                var a1 = new Animal { Id = guid1, Name = "Foo" };
                var guid2 = Guid.NewGuid();
                var a2 = new Animal { Id = guid2, Name = "Bar" };
                var guid3 = Guid.NewGuid();
                var a3 = new Animal { Id = guid3, Name = "Baz" };

                Db.InsertAsync<Animal>(new[] { a1, a2, a3 });

                var animals = Db.GetListAsync<Animal>().Result.ToList();
                Assert.AreEqual(3, animals.Count);
                Assert.IsNotNull(animals.Find(x => x.Id == guid1));
                Assert.IsNotNull(animals.Find(x => x.Id == guid2));
                Assert.IsNotNull(animals.Find(x => x.Id == guid3));
                Dispose();
            }
        }

        [TestFixture]
        public class GetMethod : SqlCeBaseFixture
        {
            [Test]
            public void UsingKey_ReturnsEntity()
            {
                var p1 = new Person
                {
                    Active = true,
                    FirstName = "Foo",
                    LastName = "Bar",
                    DateCreated = DateTime.UtcNow
                };
                var id = Db.InsertAsync(p1).Result;

                var p2 = Db.GetAsync<Person>(id).Result;
                Assert.AreEqual(id, p2.Id);
                Assert.AreEqual("Foo", p2.FirstName);
                Assert.AreEqual("Bar", p2.LastName);
                Dispose();
            }

            [Test]
            public void UsingCompositeKey_ReturnsEntity()
            {
                var m1 = new Multikey { Key2 = "key", Value = "bar" };
                var key = Db.InsertAsync(m1).Result;

                var m2 = Db.GetAsync<Multikey>(new { key.Key1, key.Key2 }).Result;
                Assert.AreEqual(1, m2.Key1);
                Assert.AreEqual("key", m2.Key2);
                Assert.AreEqual("bar", m2.Value);
                Dispose();
            }
        }

        [TestFixture]
        public class DeleteMethod : SqlCeBaseFixture
        {
            private void Arrange(out Person p1, out Person p2, out Person p3)
            {
                p1 = new Person { Active = true, FirstName = "Foo", LastName = "Bar", DateCreated = DateTime.UtcNow };
                p2 = new Person { Active = true, FirstName = "Foo", LastName = "Bar", DateCreated = DateTime.UtcNow };
                p3 = new Person { Active = true, FirstName = "Foo", LastName = "Barz", DateCreated = DateTime.UtcNow };
            }

            [Test]
            public void UsingKey_DeletesFromDatabase()
            {
                Arrange(out var p1, out var _, out var _);
                var id = Db.InsertAsync(p1).Result;

                var p2 = Db.GetAsync<Person>(id).Result;
                Assert.IsTrue(Db.DeleteAsync(p2).Result);
                Task<Person> aux = Db.GetAsync<Person>(id);

                aux.AsyncState.Should().BeNull();
                Dispose();
            }

            [Test]
            public void UsingCompositeKey_DeletesFromDatabase()
            {
                var m1 = new Multikey { Key2 = "key", Value = "bar" };
                var key = Db.InsertAsync(m1).Result;

                var m2 = Db.GetAsync<Multikey>(new { key.Key1, key.Key2 }).Result;
                Assert.IsTrue(Db.DeleteAsync(m2).Result);
                var aux = Db.GetAsync<Multikey>(new { key.Key1, key.Key2 });

                aux.AsyncState.Should().BeNull();
                Dispose();
            }

            [Test]
            public void UsingPredicate_DeletesRows()
            {
                Arrange(out var p1, out var p2, out var p3);

                Db.InsertAsync(p1);
                Db.InsertAsync(p2);
                Db.InsertAsync(p3);

                var list = Db.GetListAsync<Person>().Result;
                Assert.AreEqual(3, list.Count());

                var pred = Predicates.Field<Person>(p => p.LastName, Operator.Eq, "Bar");
                var result = Db.DeleteAsync<Person>(pred).Result;
                Assert.IsTrue(result);

                list = Db.GetListAsync<Person>().Result;
                Assert.AreEqual(1, list.Count());
                Dispose();
            }

            [Test]
            public void UsingObject_DeletesRows()
            {
                Arrange(out var p1, out var p2, out var p3);

                Db.InsertAsync(p1);
                Db.InsertAsync(p2);
                Db.InsertAsync(p3);

                var list = Db.GetListAsync<Person>().Result;
                Assert.AreEqual(3, list.Count());

                var result = Db.DeleteAsync<Person>(new { LastName = "Bar" }).Result;
                Assert.IsTrue(result);

                list = Db.GetListAsync<Person>().Result;
                Assert.AreEqual(1, list.Count());
                Dispose();
            }
        }

        [TestFixture]
        public class UpdateMethod : SqlCeBaseFixture
        {
            [Test]
            public void UsingKey_UpdatesEntity()
            {
                var p1 = new Person
                {
                    Active = true,
                    FirstName = "Foo",
                    LastName = "Bar",
                    DateCreated = DateTime.UtcNow
                };
                var id = Db.InsertAsync(p1).Result;

                var p2 = Db.GetAsync<Person>(id).Result;
                p2.FirstName = "Baz";
                p2.Active = false;

                Db.UpdateAsync(p2);

                var p3 = Db.GetAsync<Person>(id).Result;
                Assert.AreEqual("Baz", p3.FirstName);
                Assert.AreEqual("Bar", p3.LastName);
                Assert.AreEqual(false, p3.Active);
                Dispose();
            }

            [Test]
            public void UsingCompositeKey_UpdatesEntity()
            {
                var m1 = new Multikey { Key2 = "key", Value = "bar" };
                var key = Db.InsertAsync(m1).Result;

                var m2 = Db.GetAsync<Multikey>(new { key.Key1, key.Key2 }).Result;
                m2.Key2 = "key";
                m2.Value = "barz";
                Db.UpdateAsync(m2);

                var m3 = Db.GetAsync<Multikey>(new { Key1 = 1, Key2 = "key" }).Result;
                Assert.AreEqual(1, m3.Key1);
                Assert.AreEqual("key", m3.Key2);
                Assert.AreEqual("barz", m3.Value);
                Dispose();
            }
        }

        [TestFixture]
        public class GetListMethod : SqlCeBaseFixture
        {
            private void Arrange()
            {
                Db.InsertAsync(new Person { Active = true, FirstName = "a", LastName = "a1", DateCreated = DateTime.UtcNow });
                Db.InsertAsync(new Person { Active = false, FirstName = "b", LastName = "b1", DateCreated = DateTime.UtcNow });
                Db.InsertAsync(new Person { Active = true, FirstName = "c", LastName = "c1", DateCreated = DateTime.UtcNow });
                Db.InsertAsync(new Person { Active = false, FirstName = "d", LastName = "d1", DateCreated = DateTime.UtcNow });
            }

            [Test]
            public void UsingNullPredicate_ReturnsAll()
            {
                Arrange();

                IEnumerable<Person> list = Db.GetListAsync<Person>().Result;
                Assert.AreEqual(4, list.Count());
                Dispose();
            }

            [Test]
            public void UsingPredicate_ReturnsMatching()
            {
                Arrange();

                var predicate = Predicates.Field<Person>(f => f.Active, Operator.Eq, true);
                IEnumerable<Person> list = Db.GetListAsync<Person>(predicate, null).Result;
                Assert.AreEqual(2, list.Count());
                Assert.IsTrue(list.All(p => p.FirstName == "a" || p.FirstName == "c"));
                Dispose();
            }

            [Test]
            public void UsingObject_ReturnsMatching()
            {
                Arrange();

                var predicate = new { Active = true, FirstName = "c" };
                IEnumerable<Person> list = Db.GetListAsync<Person>(predicate, null).Result;
                Assert.AreEqual(1, list.Count());
                Assert.IsTrue(list.All(p => p.FirstName == "c"));
                Dispose();
            }
        }

        [TestFixture]
        public class GetPageMethod : SqlCeBaseFixture
        {
            private void Arrange(out dynamic id1, out dynamic id2, out dynamic id3, out dynamic id4)
            {
                id1 = Db.InsertAsync(new Person { Active = true, FirstName = "Sigma", LastName = "Alpha", DateCreated = DateTime.UtcNow }).Result;
                id2 = Db.InsertAsync(new Person { Active = false, FirstName = "Delta", LastName = "Alpha", DateCreated = DateTime.UtcNow }).Result;
                id3 = Db.InsertAsync(new Person { Active = true, FirstName = "Theta", LastName = "Gamma", DateCreated = DateTime.UtcNow }).Result;
                id4 = Db.InsertAsync(new Person { Active = false, FirstName = "Iota", LastName = "Beta", DateCreated = DateTime.UtcNow }).Result;
            }

            [Test]
            public void UsingNullPredicate_ReturnsMatching()
            {
                Arrange(out var id1, out var id2, out var id3, out var id4);

                var sort = new List<ISort>
                                    {
                                        Predicates.Sort<Person>(p => p.LastName),
                                        Predicates.Sort<Person>("FirstName")
                                    };

                var list = Db.GetPageAsync<Person>(null, sort, 0, 2).Result;
                Assert.AreEqual(2, list.Count());
                Assert.AreEqual(id2, list.First().Id);
                Assert.AreEqual(id1, list.Skip(1).First().Id);
                Dispose();
            }

            [Test]
            public void UsingPredicate_ReturnsMatching()
            {
                Arrange(out var id1, out var id2, out var id3, out var id4);

                var predicate = Predicates.Field<Person>(f => f.Active, Operator.Eq, true);
                var sort = new List<ISort>
                                    {
                                        Predicates.Sort<Person>(p => p.LastName),
                                        Predicates.Sort<Person>("FirstName")
                                    };

                var list = Db.GetPageAsync<Person>(predicate, sort, 0, 2).Result;
                Assert.AreEqual(2, list.Count());
                Assert.IsTrue(list.All(p => p.FirstName == "Sigma" || p.FirstName == "Theta"));
                Dispose();
            }

            [Test]
            public void NotFirstPage_Returns_NextResults()
            {
                Arrange(out var id1, out var id2, out var id3, out var id4);

                var sort = new List<ISort>
                                    {
                                        Predicates.Sort<Person>(p => p.LastName),
                                        Predicates.Sort<Person>("FirstName")
                                    };

                var list = Db.GetPageAsync<Person>(null, sort, 2, 2).Result;
                Assert.AreEqual(2, list.Count());
                Assert.AreEqual(id4, list.First().Id);
                Assert.AreEqual(id3, list.Skip(1).First().Id);
                Dispose();
            }

            [Test]
            public void UsingObject_ReturnsMatching()
            {
                Arrange(out var id1, out var id2, out var id3, out var id4);

                var predicate = new { Active = true };
                var sort = new List<ISort>
                                    {
                                        Predicates.Sort<Person>(p => p.LastName),
                                        Predicates.Sort<Person>("FirstName")
                                    };

                var list = Db.GetPageAsync<Person>(predicate, sort, 0, 2).Result;
                Assert.AreEqual(2, list.Count());
                Assert.IsTrue(list.All(p => p.FirstName == "Sigma" || p.FirstName == "Theta"));
                Dispose();
            }
        }

        [TestFixture]
        public class CountMethod : SqlCeBaseFixture
        {
            private void Arrange()
            {
                Db.InsertAsync(new Person { Active = true, FirstName = "a", LastName = "a1", DateCreated = DateTime.UtcNow.AddDays(-10) });
                Db.InsertAsync(new Person { Active = false, FirstName = "b", LastName = "b1", DateCreated = DateTime.UtcNow.AddDays(-10) });
                Db.InsertAsync(new Person { Active = true, FirstName = "c", LastName = "c1", DateCreated = DateTime.UtcNow.AddDays(-3) });
                Db.InsertAsync(new Person { Active = false, FirstName = "d", LastName = "d1", DateCreated = DateTime.UtcNow.AddDays(-1) });
            }

            [Test]
            public void UsingNullPredicate_Returns_Count()
            {
                Arrange();

                var count = Db.CountAsync<Person>(null).Result;
                Assert.AreEqual(4, count);
                Dispose();
            }

            [Test]
            public void UsingPredicate_Returns_Count()
            {
                Arrange();

                var predicate = Predicates.Field<Person>(f => f.DateCreated, Operator.Lt, DateTime.UtcNow.AddDays(-5));
                var count = Db.CountAsync<Person>(predicate).Result;
                Assert.AreEqual(2, count);
                Dispose();
            }

            [Test]
            public void UsingObject_Returns_Count()
            {
                Arrange();

                var predicate = new { FirstName = new[] { "b", "d" } };
                var count = Db.CountAsync<Person>(predicate).Result;
                Assert.AreEqual(2, count);
                Dispose();
            }
        }

        [TestFixture]
        public class GetMultipleMethod : SqlCeBaseFixture
        {
            [Test]
            public void ReturnsItems()
            {
                Db.InsertAsync(new Person { Active = true, FirstName = "a", LastName = "a1", DateCreated = DateTime.UtcNow.AddDays(-10) });
                Db.InsertAsync(new Person { Active = false, FirstName = "b", LastName = "b1", DateCreated = DateTime.UtcNow.AddDays(-10) });
                Db.InsertAsync(new Person { Active = true, FirstName = "c", LastName = "c1", DateCreated = DateTime.UtcNow.AddDays(-3) });
                Db.InsertAsync(new Person { Active = false, FirstName = "d", LastName = "d1", DateCreated = DateTime.UtcNow.AddDays(-1) });

                Db.InsertAsync(new Animal { Name = "Foo" });
                Db.InsertAsync(new Animal { Name = "Bar" });
                Db.InsertAsync(new Animal { Name = "Baz" });

                var predicate = new GetMultiplePredicate();
                predicate.Add<Person>(null);
                predicate.Add<Animal>(Predicates.Field<Animal>(a => a.Name, Operator.Like, "Ba%"));
                predicate.Add<Person>(Predicates.Field<Person>(a => a.LastName, Operator.Eq, "c1"));

                var result = Db.GetMultipleAsync(predicate).Result;
                var people = result.Read<Person>().ToList();
                var animals = result.Read<Animal>().ToList();
                var people2 = result.Read<Person>().ToList();

                people.Should().HaveCount(4);
                animals.Should().HaveCount(2);
                people2.Should().HaveCount(1);
                Dispose();
            }
        }
    }
}
#endif