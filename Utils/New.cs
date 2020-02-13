using System;
using System.Linq.Expressions;
using System.Reflection.Emit;

namespace SecurityDriven.TinyORM.Utils
{
	internal static class New<T> where T : new() // very fast object/struct factory
	{
		public static readonly Func<T> Instance = DynamicModuleLambdaCompiler.GenerateFactory<T>();
	}//class New<T>

	// https://blogs.msdn.microsoft.com/seteplia/2017/02/01/dissecting-the-new-constraint-in-c-a-perfect-example-of-a-leaky-abstraction/
	static class DynamicModuleLambdaCompiler
	{
		static System.Reflection.Module _module = typeof(DynamicModuleLambdaCompiler).Module;
		public static Func<T> GenerateFactory<T>() where T : new()
		{
			Expression<Func<T>> expr = () => new T();
			NewExpression newExpr = (NewExpression)expr.Body;

			var method = new DynamicMethod(
				name: "lambda",
				returnType: newExpr.Type,
				parameterTypes: Util.ZeroLengthArray<Type>.Value,
				m: _module,
				skipVisibility: true);

			ILGenerator ilGen = method.GetILGenerator();
			// Constructor for value types could be null
			if (newExpr.Constructor != null)
			{
				ilGen.Emit(OpCodes.Newobj, newExpr.Constructor);
			}
			else
			{
				LocalBuilder temp = ilGen.DeclareLocal(newExpr.Type);
				ilGen.Emit(OpCodes.Ldloca, temp);
				ilGen.Emit(OpCodes.Initobj, newExpr.Type);
				ilGen.Emit(OpCodes.Ldloc, temp);
			}

			ilGen.Emit(OpCodes.Ret);

			return (Func<T>)method.CreateDelegate(typeof(Func<T>));
		}// GenerateFactory<T>()
	}//class DynamicModuleLambdaCompiler
}//ns