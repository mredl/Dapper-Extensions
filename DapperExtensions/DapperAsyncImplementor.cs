using System;
using Dapper;
using DapperExtensions.Mapper;
using DapperExtensions.Predicate;
using DapperExtensions.Sql;
using System.Collections.Generic;
using System.Data;
using System.Dynamic;
using System.Linq;
using System.Threading.Tasks;

namespace DapperExtensions
{
    /// <summary>
    /// Interface for asyncImplementator
    /// </summary>
    public interface IDapperAsyncImplementor : IDapperImplementor
    {
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Get{T}"/>.
        /// </summary>
        Task<T> GetAsync<T>(IDbConnection connection, dynamic id, IDbTransaction transaction = null,
            int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetList{T}"/>.
        /// </summary>
        Task<IEnumerable<T>> GetListAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null,
            int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetListAutoMap{T}"/>.
        /// </summary>
        Task<IEnumerable<T>> GetListAutoMapAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null,
            int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPage{T}"/>.
        /// </summary>
        Task<IEnumerable<T>> GetPageAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int page = 1, int resultsPerPage = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPageAutoMap{T}"/>.
        /// </summary>
        Task<IEnumerable<T>> GetPageAutoMapAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int page = 1, int resultsPerPage = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetSet{T}"/>.
        /// </summary>
        Task<IEnumerable<T>> GetSetAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int firstResult = 1, int maxResults = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Count{T}"/>.
        /// </summary>
        Task<int> CountAsync<T>(IDbConnection connection, object predicate = null, IDbTransaction transaction = null, int? commandTimeout = null, IList<IReferenceMap> includedProperties = null);

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Insert{T}(IDbConnection, IEnumerable{T}, IDbTransaction, int?)"/>.
        /// </summary>
        Task InsertAsync<T>(IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = default);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Insert{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        Task<dynamic> InsertAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction = null, int? commandTimeout = default);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Update{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        Task<bool> UpdateAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout, bool ignoreAllKeyProperties = false, IList<IProjection> colsToUpdate = null);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Delete{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        Task<bool> DeleteAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout);
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Delete{T}(IDbConnection, object, IDbTransaction, int?)"/>.
        /// </summary>
        Task<bool> DeleteAsync<T>(IDbConnection connection, object predicate, IDbTransaction transaction, int? commandTimeout);

        Task<IMultipleResultReader> GetMultipleAsync(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction, int? commandTimeout, IList<IReferenceMap> includedProperties = null);
    }

    public partial class DapperAsyncImplementor : DapperImplementor, IDapperAsyncImplementor
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="DapperAsyncImplementor"/> class.
        /// </summary>
        /// <param name="sqlGenerator">The SQL generator.</param>
        public DapperAsyncImplementor(ISqlGenerator sqlGenerator)
            : base(sqlGenerator) { }

        #region Implementation of IDapperAsyncImplementor
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Insert{T}(IDbConnection, IEnumerable{T}, IDbTransaction, int?)"/>.
        /// </summary>
        public async Task InsertAsync<T>(IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction = null, int? commandTimeout = default)
        {
            //Got the information here to avoid doing it for each item and so we speed up the execution
            var classMap = SqlGenerator.Configuration.GetMap<T>();
            var nonIdentityKeyProperties = classMap.Properties.Where(p => p.KeyType == KeyType.Guid || p.KeyType == KeyType.Assigned).ToList();
            var identityColumn = classMap.Properties.SingleOrDefault(p => p.KeyType == KeyType.Identity);
            var triggerIdentityColumn = classMap.Properties.SingleOrDefault(p => p.KeyType == KeyType.TriggerIdentity);
            var sequenceIdentityColumn = classMap.Properties.Where(p => p.KeyType == KeyType.SequenceIdentity).ToList();

            foreach (var e in entities)
                await InternalInsertAsync(connection, e, transaction, commandTimeout, classMap, nonIdentityKeyProperties, identityColumn, triggerIdentityColumn, sequenceIdentityColumn);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Insert{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        public async Task<dynamic> InsertAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout)
        {
            var classMap = SqlGenerator.Configuration.GetMap<T>();
            var nonIdentityKeyProperties = classMap.Properties.Where(p => p.KeyType == KeyType.Guid || p.KeyType == KeyType.Assigned).ToList();
            var identityColumn = classMap.Properties.SingleOrDefault(p => p.KeyType == KeyType.Identity);
            var triggerIdentityColumn = classMap.Properties.SingleOrDefault(p => p.KeyType == KeyType.TriggerIdentity);
            var sequenceIdentityColumn = classMap.Properties.Where(p => p.KeyType == KeyType.SequenceIdentity).ToList();

            return await InternalInsertAsync(connection, entity, transaction, commandTimeout, classMap, nonIdentityKeyProperties, identityColumn, triggerIdentityColumn, sequenceIdentityColumn);
        }
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Update{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        public async Task<bool> UpdateAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout, bool ignoreAllKeyProperties, IList<IProjection> colsToUpdate = null)
        {
            return await InternalUpdateAsync(connection, entity, transaction, colsToUpdate, commandTimeout, ignoreAllKeyProperties);
        }
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Delete{T}(IDbConnection, T, IDbTransaction, int?)"/>.
        /// </summary>
        public Task<bool> DeleteAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            GetMapAndPredicate<T>(entity, out var classMap, out var predicate, true);
            return InternalDeleteAsync<T>(connection, classMap, predicate, transaction, commandTimeout);
        }
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Delete{T}(IDbConnection, object, IDbTransaction, int?)"/>.
        /// </summary>
        public Task<bool> DeleteAsync<T>(IDbConnection connection, object predicate, IDbTransaction transaction, int? commandTimeout) where T : class
        {
            GetMapAndPredicate<T>(predicate, out var classMap, out var wherePredicate);
            return InternalDeleteAsync<T>(connection, classMap, wherePredicate, transaction, commandTimeout);
        }
        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Get{T}"/>.
        /// </summary>
        public async Task<T> GetAsync<T>(IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false,
            IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            var result = (IEnumerable<T>)await InternalGetListAutoMapAsync<T>(connection, id, null, transaction, commandTimeout, true, colsToSelect, includedProperties);
            return result.SingleOrDefault();
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetList{T}"/>.
        /// </summary>
        public async Task<IEnumerable<T>> GetListAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, IDbTransaction transaction = null,
            int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetListAutoMapAsync<T>(connection, predicate, sort, transaction, commandTimeout, buffered, colsToSelect, includedProperties);
        }

        public async Task<IEnumerable<T>> GetListAutoMapAsync<T>(IDbConnection connection, object predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout,
            bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetListAutoMapAsync<T>(connection, predicate, sort, transaction, commandTimeout, buffered, colsToSelect, includedProperties);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPage{T}"/>.
        /// </summary>
        public async Task<IEnumerable<T>> GetPageAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int page = 1, int resultsPerPage = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetPageAutoMapAsync<T>(connection, predicate, sort, page, resultsPerPage, transaction, commandTimeout, buffered, colsToSelect, includedProperties);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPageAutoMap{T}"/>.
        /// </summary>
        public async Task<IEnumerable<T>> GetPageAutoMapAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int page = 1, int resultsPerPage = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetPageAutoMapAsync<T>(connection, predicate, sort, page, resultsPerPage, transaction, commandTimeout, buffered, colsToSelect, includedProperties);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetSet{T}"/>.
        /// </summary>
        public async Task<IEnumerable<T>> GetSetAsync<T>(IDbConnection connection, object predicate = null, IList<ISort> sort = null, int firstResult = 1, int maxResults = 10,
            IDbTransaction transaction = null, int? commandTimeout = null, bool buffered = false, IList<IProjection> colsToSelect = null, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetSetAsync<T>(connection, predicate, sort, firstResult, maxResults, transaction, commandTimeout, buffered, colsToSelect, includedProperties);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.Count{T}"/>.
        /// </summary>
        public async Task<int> CountAsync<T>(IDbConnection connection, object predicate = null, IDbTransaction transaction = null,
            int? commandTimeout = null, IList<IReferenceMap> includedProperties = null)
        {
            GetMapAndPredicate<T>(predicate, out var classMap, out var wherePredicate);
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.Count(classMap, wherePredicate, parameters, includedProperties);

            var dynamicParameters = GetDynamicParameters(parameters);
            LastExecutedCommand = sql;
            var result = await connection.QueryAsync(sql, dynamicParameters, transaction, commandTimeout,
                CommandType.Text);
            return (int)result.Single().Total;
        }

        public async Task<IMultipleResultReader> GetMultipleAsync(IDbConnection connection, GetMultiplePredicate predicate, IDbTransaction transaction,
            int? commandTimeout, IList<IReferenceMap> includedProperties = null)
        {
            if (SqlGenerator.SupportsMultipleStatements())
            {
                return await GetMultipleByBatchAsync(connection, predicate, transaction, commandTimeout, includedProperties);
            }

            return await GetMultipleBySequenceAsync(connection, predicate, transaction, commandTimeout, includedProperties);
        }
        #endregion

        #region Private implementations

        private async Task<dynamic> InternalInsertAsync<T>(IDbConnection connection, T entity,
            IDbTransaction transaction, int? commandTimeout,
            IClassMapper classMap, IList<IMemberMap> nonIdentityKeyProperties, IMemberMap identityColumn,
            IMemberMap triggerIdentityColumn, IList<IMemberMap> sequenceIdentityColumn)
        {
            DynamicParameters dynamicParameters = null;

            foreach (var column in nonIdentityKeyProperties)
            {
                if (column.KeyType == KeyType.Guid && (Guid)column.GetValue(entity) == Guid.Empty)
                {
                    var comb = SqlGenerator.Configuration.GetNextGuid();
                    column.SetValue(entity, comb);
                }
            }

            IDictionary<string, object> keyValues = new ExpandoObject();
            var sql = SqlGenerator.Insert(classMap);
            if (triggerIdentityColumn != null || identityColumn != null)
            {
                var keyColumn = triggerIdentityColumn ?? identityColumn;
                object keyValue;

                dynamicParameters = GetDynamicParameters(classMap, entity, dynamicParameters, keyColumn, true);

                if (triggerIdentityColumn != null)
                {
                    keyValue = InsertTriggered(connection, entity, transaction, commandTimeout, sql, triggerIdentityColumn, dynamicParameters);
                }
                else
                {
                    keyValue = InsertIdentity(connection, transaction, commandTimeout, classMap, sql, dynamicParameters);
                }

                keyValues.Add(keyColumn.Name, keyValue);
                keyColumn.SetValue(entity, keyValue);
            }
            else
            {
                dynamicParameters = GetDynamicParameters(classMap, entity, true);

                if (sequenceIdentityColumn.Count > 0)
                {
                    if (sequenceIdentityColumn.Count > 1)
                        throw new ArgumentException("SequenceIdentity generator cannot be used with multi-column keys");

                    AddSequenceParameter(connection, entity, sequenceIdentityColumn[0], dynamicParameters, keyValues);
                }
                else if (nonIdentityKeyProperties != null)
                {
                    AddKeyParameters(entity, nonIdentityKeyProperties, dynamicParameters, true);
                }

                LastExecutedCommand = sql;
                await connection.ExecuteAsync(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text);
            }

            foreach (var column in nonIdentityKeyProperties)
            {
                keyValues.Add(column.Name, column.GetValue(entity));
            }

            if (keyValues.Count == 1)
            {
                return keyValues.First().Value;
            }

            return keyValues;
        }

        private async Task<bool> InternalUpdateAsync<T>(IDbConnection connection, T entity, IClassMapper classMap,
            IPredicate predicate, IDbTransaction transaction,
            IList<IProjection> cols, int? commandTimeout, bool ignoreAllKeyProperties = false) where T : class
        {
            var parameters = new Dictionary<string, object>();
            string sql = SqlGenerator.Update(classMap, predicate, parameters, ignoreAllKeyProperties, cols);

            var dynamicParameters = GetDynamicParameters(classMap, entity, true);
            dynamicParameters.AddDynamicParams(GetDynamicParameters(parameters));

            LastExecutedCommand = sql;
            return await connection.ExecuteAsync(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text) > 0;
        }


        private async Task<bool> InternalUpdateAsync<T>(IDbConnection connection, T entity, IDbTransaction transaction, IList<IProjection> cols,
            int? commandTimeout, bool ignoreAllKeyProperties = false)
        {
            GetMapAndPredicate<T>(entity, out var classMap, out var predicate, true);
            return await InternalUpdateAsync(connection, entity, classMap, predicate, transaction, cols, commandTimeout, ignoreAllKeyProperties);
        }

        private async void InternalUpdateAsync<T>(IDbConnection connection, IEnumerable<T> entities, IDbTransaction transaction, IList<IProjection> cols,
            int? commandTimeout, bool ignoreAllKeyProperties = false)
        {
            GetMapAndPredicate<T>(entities.FirstOrDefault(), out var classMap, out var predicate, true);

            foreach (var e in entities)
                await InternalUpdateAsync(connection, e, classMap, predicate, transaction, cols, commandTimeout, ignoreAllKeyProperties);
        }

        private async Task<T> InternalGetAsync<T>(IDbConnection connection, dynamic id, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect, IList<IReferenceMap> includedProperties = null)
        {
            return await InternalGetListAutoMapAsync<T>(connection, id, null, transaction, commandTimeout, true, colsToSelect, includedProperties);
        }

        private async Task<IEnumerable<T>> InternalGetListAutoMapAsync<T>(IDbConnection connection, object predicate, IList<ISort> sort, IDbTransaction transaction,
            int? commandTimeout, bool buffered, IList<IProjection> colsToSelect, IList<IReferenceMap> includedProperties = null)
        {
            GetMapAndPredicate<T>(predicate, out var classMap, out var wherePredicate);
           
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.Select(classMap, wherePredicate, sort, parameters, colsToSelect, includedProperties);
            var dynamicParameters = GetDynamicParameters(parameters);

            LastExecutedCommand = sql;
            var command = new CommandDefinition(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text, buffered ? CommandFlags.Buffered : CommandFlags.None);
            var query = await connection.QueryAsync<T>(command);

            return query;
        }

        private async Task<IEnumerable<T>> InternalGetPageAutoMapAsync<T>(IDbConnection connection, object predicate, IList<ISort> sort, int page, int resultsPerPage,
            IDbTransaction transaction, int? commandTimeout, bool buffered, IList<IProjection> colsToSelect, IList<IReferenceMap> includedProperties = null)
        {
            GetMapAndPredicate<T>(predicate, out var classMap, out var wherePredicate);
            
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.SelectPaged(classMap, wherePredicate, sort, page, resultsPerPage, parameters, colsToSelect, includedProperties);
            var dynamicParameters = GetDynamicParameters(parameters);

            LastExecutedCommand = sql;
            var command = new CommandDefinition(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text, buffered ? CommandFlags.Buffered : CommandFlags.None);
            var query = await connection.QueryAsync<T>(command);

            return query;
        }

        private Task<IEnumerable<T>> InternalGetSetAsync<T>(IDbConnection connection, object predicate, IList<ISort> sort, int firstResult, int maxResults,
            IDbTransaction transaction, int? commandTimeout, bool buffered, IList<IProjection> colsToSelect, IList<IReferenceMap> includedProperties = null) where T : class
        {
            GetMapAndPredicate<T>(predicate, out var classMap, out var wherePredicate);
            
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.SelectSet(classMap, wherePredicate, sort, firstResult, maxResults, parameters, colsToSelect, includedProperties);
            var dynamicParameters = GetDynamicParameters(parameters);

            LastExecutedCommand = sql;
            var command = new CommandDefinition(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text, buffered ? CommandFlags.Buffered : CommandFlags.None);
            return connection.QueryAsync<T>(command);
        }
        #endregion

        #region Helpers

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetList{T}"/>.
        /// </summary>
        protected async Task<IEnumerable<T>> GetListAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect = null)
        {
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.Select(classMap, predicate, sort, parameters, colsToSelect);
            var dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return await connection.QueryAsync<T>(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text).ConfigureAwait(false);
        }

        protected async Task<IEnumerable<T>> GetListAutoMapAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect = null)
        {
            var query = await GetListAsync<T>(connection, classMap, predicate, sort, transaction, commandTimeout, colsToSelect);
            var data = query.ToList();

            return data;
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPage{T}"/>.
        /// </summary>
        protected async Task<IEnumerable<T>> GetPageAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int page, int resultsPerPage, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect = null)
        {
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.SelectPaged(classMap, predicate, sort, page, resultsPerPage, parameters, colsToSelect);
            var dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return await connection.QueryAsync<T>(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text).ConfigureAwait(false);
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetPage{T}"/>.
        /// </summary>
        protected async Task<IEnumerable<T>> GetPageAutoMapAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int page, int resultsPerPage, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect = null)
        {
            var query = await GetPageAsync<T>(connection, classMap, predicate, sort, page, resultsPerPage, transaction, commandTimeout, colsToSelect);
            var data = query.ToList();

            return data;
        }

        /// <summary>
        /// The asynchronous counterpart to <see cref="IDapperImplementor.GetSet{T}"/>.
        /// </summary>
        protected async Task<IEnumerable<T>> GetSetAsync<T>(IDbConnection connection, IClassMapper classMap, IPredicate predicate, IList<ISort> sort, int firstResult, int maxResults, IDbTransaction transaction, int? commandTimeout, IList<IProjection> colsToSelect = null)
        {
            var parameters = new Dictionary<string, object>();
            var sql = SqlGenerator.SelectSet(classMap, predicate, sort, firstResult, maxResults, parameters, colsToSelect);
            var dynamicParameters = new DynamicParameters();
            foreach (var parameter in parameters)
            {
                dynamicParameters.Add(parameter.Key, parameter.Value);
            }

            return await connection.QueryAsync<T>(sql, dynamicParameters, transaction, commandTimeout, CommandType.Text).ConfigureAwait(false);
        }
        #endregion
    }
}
