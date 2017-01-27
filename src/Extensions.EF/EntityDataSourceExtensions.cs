using System.ComponentModel;

namespace JosephGuadagno.Extensions.EF
{
    /// <summary>
    ///     Extension methods around the <see cref="EntityDataSource" />
    /// </summary>
    public static class EntityDataSourceExtensions
    {
        public static TEntity GetItemObject<TEntity>(object dataItem) where TEntity : class
        {
            var entity = dataItem as TEntity;

            if (entity != null) return entity;

            var td = dataItem as ICustomTypeDescriptor;
            return (TEntity) td?.GetPropertyOwner(null);
        }
    }
}