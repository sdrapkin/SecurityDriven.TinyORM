using System;
using System.Dynamic;
using System.Linq.Expressions;
using System.Reflection;

namespace SecurityDriven.TinyORM
{
	// based on Dapper's approach
	sealed class RowStoreMetaObject : DynamicMetaObject
	{
		static readonly MethodInfo s_getValueMethod;
		static readonly MethodInfo s_setValueMethod;

		static RowStoreMetaObject()
		{
			var rowStoreType = typeof(RowStore);
			var rowStoreIndexerPropertyInfo = rowStoreType.GetProperty("Item", new Type[] { typeof(string) });
			s_getValueMethod = rowStoreIndexerPropertyInfo.GetGetMethod(nonPublic: false);
			s_setValueMethod = rowStoreIndexerPropertyInfo.GetSetMethod(nonPublic: false);
		}

		public RowStoreMetaObject(Expression expression, BindingRestrictions restrictions) : base(expression, restrictions) { }
		public RowStoreMetaObject(Expression expression, BindingRestrictions restrictions, object value) : base(expression, restrictions, value) { }

		public sealed override DynamicMetaObject BindGetMember(GetMemberBinder binder)
		{
			var parameters = new Expression[] { Expression.Constant(binder.Name) };

			var callMethod = new DynamicMetaObject(
				Expression.Call(
					Expression.Convert(Expression, LimitType),
					s_getValueMethod,
					parameters),
				BindingRestrictions.GetTypeRestriction(Expression, LimitType)
				);

			return callMethod;
		}// BindGetMember()

		// Needed for Visual basic dynamic support
		public sealed override DynamicMetaObject BindInvokeMember(InvokeMemberBinder binder, DynamicMetaObject[] args)
		{
			var parameters = new Expression[] { Expression.Constant(binder.Name) };

			var callMethod = new DynamicMetaObject(
				Expression.Call(
					Expression.Convert(Expression, LimitType),
					s_getValueMethod,
					parameters),
				BindingRestrictions.GetTypeRestriction(Expression, LimitType)
				);

			return callMethod;
		}// BindInvokeMember()

		public sealed override DynamicMetaObject BindSetMember(SetMemberBinder binder, DynamicMetaObject value)
		{
			var objectType = typeof(object);
			var parameters = new Expression[] { Expression.Constant(binder.Name), Expression.Convert(value.Expression, objectType) };
			var callMethod = new DynamicMetaObject(
			Expression.Block(
				Expression.Call(
					Expression.Convert(Expression, LimitType),
					 s_setValueMethod,
					parameters
				), Expression.Default(objectType)),
				 BindingRestrictions.GetTypeRestriction(Expression, LimitType)
			);

			return callMethod;
		}// BindSetmember()
	}// class RowStoreMetaObject
}//ns
