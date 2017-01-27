//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Data;
using System.Data.Common;
using System.Data.Entity;
using System.Data.Entity.Core;
using System.Data.Entity.Core.EntityClient;
using System.Data.Entity.Core.Objects;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using JosephGuadagno.Extensions.EF.Resources;

namespace JosephGuadagno.Extensions.EF
{
    /// <summary>
    ///     Helper methods for System.Data and System.Data.Entity types.
    /// </summary>
    public static class DataExtensions
    {
        /// <summary>
        ///     Returns the value for a field in the given data record.
        /// </summary>
        /// <remarks>
        ///     If this method is used within a Materializer expression, it will
        ///     be optimized. If the 'name' argument is a literal, we replace it with
        ///     the column ordinal. In addition, the optimizations described for
        ///     Field(record, ordinal) are applied.
        /// </remarks>
        /// <typeparam name="T">Expected type of the field.</typeparam>
        /// <param name="record">Record from which to retrieve field.</param>
        /// <param name="name">Name of field.</param>
        /// <returns>Value of field.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        [MaterializerOptimizedMethod(typeof(FieldMethodOptimizer))]
        public static T Field<T>(this IDataRecord record, string name)
        {
            Validation.CheckArgumentNotNull(record, "record");
            Validation.CheckArgumentNotNull(name, "name");

            return Field<T>(record, record.GetOrdinal(name).ToString());
        }

        /// <summary>
        ///     Returns the value for a field in the given data record.
        /// </summary>
        /// <remarks>
        ///     If this method is used within a Materializer expression, it will
        ///     be optimized. The record is directly accessed and IsDBNull checks
        ///     are included only if warranted by T.
        /// </remarks>
        /// <typeparam name="T">Expected type of the field.</typeparam>
        /// <param name="record">Record from which to retrieve field.</param>
        /// <param name="ordinal">Ordinal of field.</param>
        /// <returns>Value of field.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        [MaterializerOptimizedMethod(typeof(FieldMethodOptimizer))]
        public static T Field<T>(this IDataRecord record, int ordinal)
        {
            Validation.CheckArgumentNotNull(record, "record");

            object value = record.IsDBNull(ordinal) ? null : record.GetValue(ordinal);

            return (T) value;
        }

        /// <summary>
        ///     Uses default Materializer behaviors to shape reader results as an enumeration
        ///     of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="reader">Data reader producing results.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static IEnumerable<T> Materialize<T>(this DbDataReader reader)
        {
            return new Materializer<T>().Materialize(reader);
        }

        /// <summary>
        ///     Uses the given expression to shape reader results as an enumeration of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="reader">Data reader producing results.</param>
        /// <param name="expression">Expression describing how reader results should be shaped.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static IEnumerable<T> Materialize<T>(this DbDataReader reader,
            Expression<Func<IDataRecord, T>> expression)
        {
            return new Materializer<T>(expression).Materialize(reader);
        }

        /// <summary>
        ///     Uses default Materializer behaviors to shape the result of evaluating the given command as
        ///     an enumeration of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="command">Command producing results.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static IEnumerable<T> Materialize<T>(this DbCommand command)
        {
            return new Materializer<T>().Materialize(command);
        }

        /// <summary>
        ///     Uses the given expression to shape command results as an enumeration of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="command">Data reader producing results.</param>
        /// <param name="commandBehavior">Command behavior to use when executing the command.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1004:GenericMethodsShouldProvideTypeParameter")]
        public static IEnumerable<T> Materialize<T>(this DbCommand command, CommandBehavior commandBehavior)
        {
            return new Materializer<T>().Materialize(command, commandBehavior);
        }

        /// <summary>
        ///     Uses the given expression to shape command results as an enumeration of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="command">Data reader producing results.</param>
        /// <param name="expression">Expression describing how reader results should be shaped.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static IEnumerable<T> Materialize<T>(this DbCommand command, Expression<Func<IDataRecord, T>> expression)
        {
            return new Materializer<T>(expression).Materialize(command);
        }

        /// <summary>
        ///     Uses the given expression to shape command results as an enumeration of T.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="command">Data reader producing results.</param>
        /// <param name="commandBehavior">Command behavior to use when executing the command.</param>
        /// <param name="expression">Expression describing how reader results should be shaped.</param>
        /// <returns>Typed reader results.</returns>
        [SuppressMessage("Microsoft.Design", "CA1006:DoNotNestGenericTypesInMemberSignatures")]
        public static IEnumerable<T> Materialize<T>(this DbCommand command, CommandBehavior commandBehavior,
            Expression<Func<IDataRecord, T>> expression)
        {
            return new Materializer<T>(expression).Materialize(command, commandBehavior);
        }

        /// <summary>
        ///     Creates a store command based on the connection used by given object context.
        /// </summary>
        /// <param name="context">Object context.</param>
        /// <param name="commandText">Command text.</param>
        /// <param name="parameters">Parameters to pass to the store command.</param>
        /// <returns>Store command.</returns>
        public static DbCommand CreateStoreCommand(this ObjectContext context, string commandText,
            params object[] parameters)
        {
            return CreateStoreCommand(context, commandText, CommandType.Text, parameters);
        }

        /// <summary>
        ///     Creates a store command based on the connection used by given object context.
        /// </summary>
        /// <param name="context">Object context.</param>
        /// <param name="commandText">Command text.</param>
        /// <param name="commandType">Command type.</param>
        /// <param name="parameters">Parameters to pass to the store command.</param>
        /// <returns>Store command.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0"),
         SuppressMessage("Microsoft.Security", "CA2100:Review SQL queries for security vulnerabilities")]
        public static DbCommand CreateStoreCommand(this ObjectContext context, string commandText,
            CommandType commandType, params object[] parameters)
        {
            Validation.CheckArgumentNotNull(context, "context");

            EntityConnection entityConnection = (EntityConnection) context.Connection;
            DbConnection storeConnection = entityConnection.StoreConnection;
            DbCommand storeCommand = storeConnection.CreateCommand();

            // setup command
            storeCommand.CommandText = commandText;
            storeCommand.CommandType = commandType;
            if (null != parameters)
            {
                storeCommand.Parameters.AddRange(parameters);
            }

            // pass through command timeout as appropriate
            if (context.CommandTimeout.HasValue)
            {
                storeCommand.CommandTimeout = context.CommandTimeout.Value;
            }

            return storeCommand;
        }

        /// <summary>
        ///     Associates results in the given source with an entity set. This supports tracking
        ///     results in the state manager. If an existing element with the same key exists, it
        ///     is returned instead.
        /// </summary>
        /// <typeparam name="TEntity">Entity type.</typeparam>
        /// <typeparam name="TBase">Base type for entity set.</typeparam>
        /// <param name="source">Entities to bind.</param>
        /// <param name="objectSet">Entity set to which elements should be bound.</param>
        /// <returns>Bound entities.</returns>
        public static IEnumerable<TEntity> Bind<TEntity, TBase>(this IEnumerable<TEntity> source,
            ObjectSet<TBase> objectSet)
            where TEntity : class, TBase
            where TBase : class
        {
            Validation.CheckArgumentNotNull(source, "source");
            Validation.CheckArgumentNotNull(objectSet, "objectSet");

            return source.Select(objectSet.FindOrAttach);
        }

        /// <summary>
        ///     Associates results in the given source with the default entity set for the given entity type.
        ///     This supports tracking results in the state manager. If an existing element with the same key
        ///     exists, it is returned instead.
        /// </summary>
        /// <typeparam name="TEntity">Entity type.</typeparam>
        /// <param name="source">Entities to bind.</param>
        /// <param name="context">Object context to which entities are bound.</param>
        /// <returns>Bound entities.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "1")]
        public static IEnumerable<TEntity> Bind<TEntity>(this IEnumerable<TEntity> source, ObjectContext context)
            where TEntity : class
        {
            Validation.CheckArgumentNotNull(source, "source");
            Validation.CheckArgumentNotNull(context, "context");

            return source.Bind(context.CreateObjectSet<TEntity>());
        }

        /// <summary>
        ///     Returns a handle on an IDisposable that can be used to safely control the lifetime
        ///     of an open connection. If the connection is closed, it will be opened immediately
        ///     and closed when the result of this method (the scope) is disposed. If the connection is already
        ///     open, it remains open.
        ///     <code>
        /// // Example with CreateConnectionScope
        /// using (command.Connection.CreateConnectionScope())
        /// {
        ///     command.ExecuteNonQuery();
        /// }
        /// 
        /// // Example without
        /// bool connectionOpened = command.Connection.State == ConnectionState.Closed;
        /// if (connectionOpened)
        /// {
        ///     command.Connection.Open();
        /// }
        /// try
        /// {
        ///     command.ExecuteNonQuery();
        /// }
        /// finally
        /// {
        ///     if (connectionOpened &amp;&amp; command.Connection.State == ConnectionState.Open)
        ///     {
        ///         command.Connection.Close();
        ///     }
        /// }
        /// </code>
        /// </summary>
        /// <param name="connection">Connection to open.</param>
        /// <returns>Scope closing the connection on dispose.</returns>
        public static IDisposable CreateConnectionScope(this DbConnection connection)
        {
            Validation.CheckArgumentNotNull(connection, "connection");

            return new OpenConnectionLifetime(connection);
        }

        /// <summary>
        ///     Expands all InvocationExpression instances within the given expression.
        /// </summary>
        /// <param name="expression">Expression to expand.</param>
        /// <returns>Expanded expression.</returns>
        public static Expression ExpandInvocations(this Expression expression)
        {
            return InvocationExpander.Expand(expression);
        }

        /// <summary>
        ///     Expands all InvocationExpression instances within the given query.
        /// </summary>
        /// <param name="query">Query to expand.</param>
        /// <returns>Expanded query.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static IQueryable<TElement> ExpandInvocations<TElement>(this IQueryable<TElement> query)
        {
            Validation.CheckArgumentNotNull(query, "query");

            return query.Provider.CreateQuery<TElement>(query.Expression.ExpandInvocations());
        }

        /// <summary>
        ///     Attaches the given entity or returns existing entity with the same key.
        /// </summary>
        /// <typeparam name="TElement">Element type of object set.</typeparam>
        /// <typeparam name="TEntity">Entity type.</typeparam>
        /// <param name="objectSet">Object set.</param>
        /// <param name="entity">Entity to attach.</param>
        /// <returns>Input entity or existing entity with the same key.</returns>
        [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
        public static TEntity FindOrAttach<TElement, TEntity>(this ObjectSet<TElement> objectSet, TEntity entity)
            where TEntity : class, TElement
            where TElement : class
        {
            Validation.CheckArgumentNotNull(objectSet, "objectSet");
            if (null == entity)
            {
                return null;
            }
            string qualifiedEntitySetName = objectSet.EntitySet.EntityContainer.Name + "." + objectSet.EntitySet.Name;
            EntityKey entityKey = objectSet.Context.CreateEntityKey(qualifiedEntitySetName, entity);
            ObjectStateEntry existingStateEntry;
            if (objectSet.Context.ObjectStateManager.TryGetObjectStateEntry(entityKey, out existingStateEntry) &&
                null != existingStateEntry.Entity) // A proxy entry may exist for the entity instance
            {
                try
                {
                    return (TEntity) existingStateEntry.Entity;
                }
                catch (InvalidCastException)
                {
                    throw new InvalidOperationException(Messages.AttachedEntityHasWrongType);
                }
            }
            objectSet.Attach(entity);
            return entity;
        }

        /// <summary>
        ///     Gets all members of the ObjectSet that are currently in memory.
        /// </summary>
        /// <returns>All tracked members of the ObjectSet</returns>
        public static IEnumerable<TElement> GetTrackedEntities<TElement>(this ObjectSet<TElement> objectSet)
            where TElement : class
        {
            Validation.CheckArgumentNotNull(objectSet, "objectSet");

            return GetTrackedEntities(objectSet, ~EntityState.Detached);
        }

        /// <summary>
        ///     Gets all members of the ObjectSet that are currently in memory
        ///     with the given state(s).
        /// </summary>
        /// <param name="objectSet">Object set.</param>
        /// <param name="state">Entity state flags.</param>
        /// <typeparam name="TElement">Element type of object set.</typeparam>
        /// <returns>Tracked members of the ObjectSet in the given state.</returns>
        public static IEnumerable<TElement> GetTrackedEntities<TElement>(this ObjectSet<TElement> objectSet,
            EntityState state)
            where TElement : class
        {
            Validation.CheckArgumentNotNull(objectSet, "objectSet");

            return objectSet.Context.ObjectStateManager.GetObjectStateEntries(state)
                .Where(entry => IsMemberOfObjectSet(objectSet, entry)).Select(e => e.Entity).Cast<TElement>();
        }

        private static bool IsMemberOfObjectSet<TElement>(ObjectSet<TElement> objectSet, ObjectStateEntry entry)
            where TElement : class
        {
            return !entry.IsRelationship // must be an entity
                   && null != entry.Entity // must not be a key entry
                   && entry.EntitySet == objectSet.EntitySet; // must belong to the current set
        }

        /// <summary>
        ///     Expands invocation expressions by replacing parameters with values.
        /// </summary>
        private sealed class InvocationExpander : ExpressionVisitor
        {
            private static readonly InvocationExpander s_singleton = new InvocationExpander();
            private readonly Expression _expansion;
            private readonly ParameterExpression _parameter;
            private readonly InvocationExpander _previous;

            private InvocationExpander(ParameterExpression parameter, Expression expansion, InvocationExpander previous)
            {
                Validation.CheckArgumentNotNull(parameter, "parameter");
                Validation.CheckArgumentNotNull(expansion, "expansion");
                Validation.CheckArgumentNotNull(previous, "previous");

                _parameter = parameter;
                _expansion = expansion;
                _previous = previous;
            }

            private InvocationExpander()
            {
            }

            internal static Expression Expand(Expression expression)
            {
                return s_singleton.Visit(expression);
            }

            protected override Expression VisitParameter(ParameterExpression p)
            {
                InvocationExpander expander = this;
                while (null != expander)
                {
                    if (expander._parameter == p)
                    {
                        return Visit(expander._expansion);
                    }
                    expander = expander._previous;
                }
                return base.VisitParameter(p);
            }

            [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "0")]
            protected override Expression VisitInvocation(InvocationExpression iv)
            {
                if (iv.Expression.NodeType == ExpressionType.Lambda)
                {
                    LambdaExpression lambda = (LambdaExpression) iv.Expression;
                    return lambda
                        .Parameters
                        // zip together parameters and the corresponding argument values
                        .Zip(iv.Arguments, (p, e) => new {Parameter = p, Expansion = e})
                        // add to the stack of available parameters bindings (this class doubles as an immutable stack)
                        .Aggregate(this,
                            (previous, pair) => new InvocationExpander(pair.Parameter, pair.Expansion, previous))
                        // visit the body of the lambda using an expander including the new parameter bindings
                        .Visit(lambda.Body);
                }
                return base.VisitInvocation(iv);
            }
        }

        /// <summary>
        ///     Optimizer implementation that rewrites calls to the DataExtensions.Field methods.
        /// </summary>
        private class FieldMethodOptimizer : IMaterializerMethodOptimizer
        {
            private static readonly MethodInfo s_fieldOrdinalMethod = typeof(DataExtensions).GetMethod("Field",
                new[] {typeof(IDataRecord), typeof(int)});

            private static readonly MethodInfo s_fieldNameMethod = typeof(DataExtensions).GetMethod("Field",
                new[] {typeof(IDataRecord), typeof(string)});

            private static readonly MethodInfo s_isDBNull = typeof(IDataRecord).GetMethod("IsDBNull",
                new[] {typeof(int)});

            private static readonly MethodInfo s_getValue = typeof(IDataRecord).GetMethod("GetValue",
                new[] {typeof(int)});

            [SuppressMessage("Microsoft.Design", "CA1062:Validate arguments of public methods", MessageId = "2")]
            public Expression OptimizeMethodCall(ReadOnlyCollection<string> fieldNames,
                ParameterExpression recordParameter, MethodCallExpression methodCall)
            {
                Validation.CheckArgumentNotNull(fieldNames, "fieldNames");
                Validation.CheckArgumentNotNull(methodCall, "methodCall");

                MethodPattern pattern = GetMethodPattern(methodCall);
                if (pattern == MethodPattern.Unsupported)
                {
                    // Cannot optimize this method.
                    return methodCall;
                }

                // if the input record (arguments[0]) is not the record parameter, we cannot
                // leverage field names to optimize the shaper
                if (recordParameter != methodCall.Arguments[0])
                {
                    return methodCall;
                }

                Expression ordinalExpression;
                if (!TryGetOrdinalExpression(fieldNames, methodCall, pattern, out ordinalExpression))
                {
                    return methodCall;
                }

                Type returnType = methodCall.Method.GetGenericArguments().Single();
                bool canAssignNull = returnType.IsClass ||
                                     (returnType.IsGenericType &&
                                      typeof(Nullable<>) == returnType.GetGenericTypeDefinition());

                // argument[0].GetValue(ordinal)
                Expression result = Expression.Call(methodCall.Arguments[0], s_getValue, ordinalExpression);

                if (canAssignNull)
                {
                    // (returnType)(argument[0].IsDBNull(ordinal) ? null : result)
                    result = Expression.Condition(
                        Expression.Call(methodCall.Arguments[0], s_isDBNull, ordinalExpression),
                        Expression.Constant(null, typeof(object)),
                        result);
                }

                // (returnType)result
                result = Expression.Convert(result, returnType);

                return result;
            }

            private static MethodPattern GetMethodPattern(MethodCallExpression methodCall)
            {
                // check that we have one of the expected methods
                if (!methodCall.Method.IsGenericMethod)
                {
                    // Field<> overloads are generic.
                    return MethodPattern.Unsupported;
                }
                MethodInfo genericMethod = methodCall.Method.GetGenericMethodDefinition();
                if (genericMethod == s_fieldOrdinalMethod)
                {
                    return MethodPattern.FieldOrdinal;
                }

                if (genericMethod == s_fieldNameMethod)
                {
                    return MethodPattern.FieldName;
                }

                return MethodPattern.Unsupported;
            }

            private static bool TryGetOrdinalExpression(ReadOnlyCollection<string> fieldNames,
                MethodCallExpression methodCall, MethodPattern pattern, out Expression ordinalExpression)
            {
                ordinalExpression = null;

                // check if we can determine an expression defining the ordinal
                if (pattern == MethodPattern.FieldOrdinal)
                {
                    ordinalExpression = methodCall.Arguments[1];
                    return true;
                }
                if (pattern == MethodPattern.FieldName)
                {
                    // check for a constant name that appears in the field names
                    Expression nameArgument = methodCall.Arguments[1];
                    if (nameArgument.NodeType == ExpressionType.Constant)
                    {
                        string name = (string) ((ConstantExpression) nameArgument).Value;
                        if (null == name)
                        {
                            // cannot retrieve the ordinal if we don't know the name
                            return false;
                        }
                        int ordinal = 0;
                        for (; ordinal < fieldNames.Count && name != fieldNames[ordinal]; ordinal++) ;
                        if (ordinal < fieldNames.Count)
                        {
                            ordinalExpression = Expression.Constant(ordinal);
                            return true;
                        }
                    }
                }

                return false;
            }

            private enum MethodPattern
            {
                Unsupported,
                FieldOrdinal,
                FieldName
            }
        }

        /// <summary>
        ///     Class supporting the DataExtensions.OpenConnection method.
        /// </summary>
        private class OpenConnectionLifetime : IDisposable
        {
            private readonly bool closeOnDispose;
            private readonly DbConnection connection;

            internal OpenConnectionLifetime(DbConnection connection)
            {
                this.connection = connection;
                closeOnDispose = connection.State == ConnectionState.Closed;
                if (closeOnDispose)
                {
                    this.connection.Open();
                }
            }

            public void Dispose()
            {
                if (closeOnDispose && connection.State == ConnectionState.Open)
                {
                    connection.Close();
                }
                GC.SuppressFinalize(this);
            }
        }
    }
}