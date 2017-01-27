//----------------------------------------------------------------
// Copyright (c) Microsoft Corporation.  All rights reserved.
//----------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data.Entity.Core.Objects;
using System.Data.Entity.Core.Objects.DataClasses;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using JosephGuadagno.Extensions.EF.Resources;

namespace JosephGuadagno.Extensions.EF
{
    /// <summary>
    ///     When using LINQ to Entities, queries are instances of ObjectQuery but are statically
    ///     typed as IQueryable. This makes it difficult to access members of ObjectQuery, particularly
    ///     when the element type is anonymous. These extensions make it easier to access ObjectQuery
    ///     members.
    /// </summary>
    /// <example>
    ///     <code>
    /// var q = from p in context.Products
    ///         where p.ProductName.StartsWith("Foo")
    ///         select p;
    /// 
    /// // before
    /// string commandText = ((ObjectQuery&lt;Product&gt;)q).ToTraceString();
    /// 
    /// // after
    /// string commandText = q.ToTraceString();
    /// </code>
    /// </example>
    public static class ObjectQueryExtensions
    {
        /// <summary>
        ///     Returns the given IQuerable instance as an ObjectQuery instance.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="source">IQueryable instance.</param>
        /// <returns>source as an ObjectQuery</returns>
        public static ObjectQuery<T> AsObjectQuery<T>(this IQueryable<T> source)
        {
            return source as ObjectQuery<T>;
        }

        /// <summary>
        ///     Retrieves Entity Framework trace information for the given query.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="source">IQueryable instance. Must be an ObjectQuery instance at runtime.</param>
        /// <returns>Trace string for the query.</returns>
        public static string ToTraceString<T>(this IQueryable<T> source)
        {
            return source.ToObjectQuery("source").ToTraceString();
        }

        /// <summary>
        ///     Includes navigation path in query result.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="source">IQueryable instance. Must be an ObjectQuery instance at runtime.</param>
        /// <param name="path">Navigation path.</param>
        /// <returns>Query with spanned navigations.</returns>
        public static IQueryable<T> Include<T>(this IQueryable<T> source, string path)
        {
            return source.ToObjectQuery("source").Include(path);
        }

        //public static ObjectQuery<T> Include<T>(this ObjectQuery<T> query, Expression<Func<T, object>> selector)
        //{
        //    string path = new PropertyPathVisitor().GetPropertyPath(selector);
        //    return query.Include(path);
        //}

        /// <summary>
        ///     Sets merge option for a query.
        /// </summary>
        /// <typeparam name="T">Element type.</typeparam>
        /// <param name="source">IQueryable instance. Must be an ObjectQuery instance at runtime.</param>
        /// <param name="mergeOption">Merge option to use when evaluating the query.</param>
        /// <returns>Query with merge option set.</returns>
        public static IQueryable<T> SetMergeOption<T>(this IQueryable<T> source, MergeOption mergeOption)
        {
            ObjectQuery<T> result = source.ToObjectQuery("source");
            result.MergeOption = mergeOption;
            return result;
        }

        /// <summary>
        ///     Returns binding list for the given query instance.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="source"></param>
        /// <returns></returns>
        public static IBindingList ToBindingList<T>(this IQueryable<T> source)
        {
            Validation.CheckArgumentNotNull(source, "source");
            IListSource listSource = source as IListSource;
            if (null == listSource)
            {
                throw new ArgumentException(Messages.UnableToGetBindingList, "source");
            }
            IBindingList bindingList = listSource.GetList() as IBindingList;
            if (null == bindingList)
            {
                throw new ArgumentException(Messages.UnableToGetBindingList, "source");
            }
            return bindingList;
        }

        private static ObjectQuery<T> ToObjectQuery<T>(this IQueryable<T> source, string argumentName)
        {
            Validation.CheckArgumentNotNull(source, "source");
            ObjectQuery<T> result = source as ObjectQuery<T>;
            if (null == result)
            {
                throw new ArgumentException(string.Format(CultureInfo.CurrentCulture,
                    Messages.OperationRequiresObjectQuery, argumentName));
            }
            return result;
        }

        public static ObjectQuery<T> Include<T>(this ObjectQuery<T> mainQuery, Expression<Func<T, object>> subSelector)
        {
            return mainQuery.Include(FuncToString(subSelector.Body));
        }

        public static TK Include<T, TK>(this EntityCollection<T> mainQuery, Expression<Func<T, object>> subSelector)
            where T : EntityObject, IEntityWithRelationships
            where TK : class
        {
            return null;
        }

        public static TK Include<T, TK>(this ICollection<T> mainQuery, Expression<Func<T, object>> subSelector)
            where T : class, IEntityWithRelationships
            where TK : class
        {
            return null;
        }

        public static TK Include<T, TK>(this T mainQuery, Expression<Func<T, object>> subSelector)
            where T : EntityObject
            where TK : class
        {
            return null;
        }

        private static string FuncToString(Expression selector)
        {
            switch (selector.NodeType)
            {
                case ExpressionType.MemberAccess:
                    return ((MemberExpression) selector).Member.Name;

                case ExpressionType.Call:
                    var method = selector as MethodCallExpression;
                    if (method != null)
                        return FuncToString(method.Arguments[0]) + "." + FuncToString(method.Arguments[1]);
                    break;
                case ExpressionType.Quote:
                    return FuncToString(((LambdaExpression) ((UnaryExpression) selector).Operand).Body);
            }
            throw new InvalidOperationException();
        }


        private class PropertyPathVisitor : ExpressionVisitor
        {
            private Stack<string> _stack;

            public string GetPropertyPath(Expression expression)
            {
                _stack = new Stack<string>();
                Visit(expression);
                return _stack
                    .Aggregate(
                        new StringBuilder(),
                        (sb, name) =>
                                (sb.Length > 0 ? sb.Append(".") : sb).Append(name))
                    .ToString();
            }

            protected override Expression VisitMember(MemberExpression expression)
            {
                if (_stack != null)
                    _stack.Push(expression.Member.Name);
                return base.VisitMember(expression);
            }

            protected override Expression VisitMethodCall(MethodCallExpression expression)
            {
                if (IsLinqOperator(expression.Method))
                {
                    for (int i = 1; i < expression.Arguments.Count; i++)
                    {
                        Visit(expression.Arguments[i]);
                    }
                    Visit(expression.Arguments[0]);
                    return expression;
                }
                return base.VisitMethodCall(expression);
            }

            private static bool IsLinqOperator(MethodInfo method)
            {
                if (method.DeclaringType != typeof(Queryable) && method.DeclaringType != typeof(Enumerable))
                    return false;
                return Attribute.GetCustomAttribute(method, typeof(ExtensionAttribute)) != null;
            }
        }
    }
}