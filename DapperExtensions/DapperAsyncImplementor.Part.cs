using System.Collections.Generic;
using System.Data;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using DapperExtensions.Mapper;
using DapperExtensions.Predicate;

namespace DapperExtensions
{
    public partial class DapperAsyncImplementor
    {
        private async Task<bool> InternalDeleteAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IDbTransaction transaction, int? commandTimeout) where T : class
         {
             var parameters = new Dictionary<string, object>();
             var sql = SqlGenerator.Delete(classMap, predicate, parameters);
             var dynamicParameters = GetDynamicParameters(parameters);

             LastExecutedCommand = sql;
             return await connection.ExecuteAsync(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text) > 0;
         }
      
         protected async Task<GridReaderResultReader> GetMultipleByBatchAsync(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout, IList<IReferenceMap> includedProperties = null)
        {
            var parameters = new Dictionary<string, object>();
            var sql = new StringBuilder();
            foreach (var item in predicate.Items)
            {
                var classMap = SqlGenerator.Configuration.GetMap(item.Type);
                var itemPredicate = item.Value as IPredicate;
                if (itemPredicate == null && item.Value != null)
                {
                    itemPredicate = GetPredicate(classMap, item.Value);
                }

                _ = sql.Append(SqlGenerator.Select(classMap, itemPredicate, item.Sort, parameters, null, includedProperties)).AppendLine(SqlGenerator.Configuration.Dialect.BatchSeperator);
            }

            var dynamicParameters = GetDynamicParameters(parameters);

            var grid = await connection.QueryMultipleAsync(sql.ToString(), dynamicParameters, transaction, commandTimeout, CommandType.Text);
            return new GridReaderResultReader(grid);
        }

        protected async Task<SequenceReaderResultReader> GetMultipleBySequenceAsync(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout, IList<IReferenceMap> includedProperties = null)
        {
            IList<SqlMapper.GridReader> items = new List<SqlMapper.GridReader>();
            foreach (var item in predicate.Items)
            {
                var parameters = new Dictionary<string, object>();
                var classMap = SqlGenerator.Configuration.GetMap(item.Type);
                var itemPredicate = item.Value as IPredicate;
                if (itemPredicate == null && item.Value != null)
                {
                    itemPredicate = GetPredicate(classMap, item.Value);
                }

                var sql = SqlGenerator.Select(classMap, itemPredicate, item.Sort, parameters, null, includedProperties);
                var dynamicParameters = GetDynamicParameters(parameters);

                var queryResult = await connection.QueryMultipleAsync(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text);
                items.Add(queryResult);
            }

            return new SequenceReaderResultReader(items);
        }
    }
}