using System;
using System.Collections.Generic;
using System.Dynamic;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM
{
	using Utils;

	#region struct RowStore
	public readonly struct RowStore : IReadOnlyDictionary<string, object>, IDynamicMetaObjectProvider
	{
		static readonly FieldNotFound notFound = new FieldNotFound();
		public readonly object[] RowValues;

		internal RowStore(object[] rowValues) => this.RowValues = rowValues;

		public ResultSetSchema Schema => (ResultSetSchema)this.RowValues[this.RowValues.Length - 1];

		public IEnumerable<string> Keys => this.Schema.FieldNames;

		public IEnumerable<object> Values => new ArraySegment<object>(this.RowValues, 0, this.RowValues.Length - 1);

		public int Count => this.RowValues.Length - 1;

		public object this[int i]
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get
			{
				if (i == (this.RowValues.Length - 1)) ThrowIndexOutOfRangeException();
				var result = this.RowValues[i];
				return result == DBNull.Value ? null : result;

				void ThrowIndexOutOfRangeException() => throw new IndexOutOfRangeException("Index was outside the bounds of the array.");
			}
		}

		public object this[string key]
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get
			{
				if (!TryGetIndex(key, out var index))
				{
					return notFound;
				}
				var result = this.RowValues[index];
				return result == DBNull.Value ? null : result;
			}//get
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			set
			{
				if (!TryGetIndex(key, out var index))
				{
					ThrowArgumentException();
				}
				this.RowValues[index] = value ?? DBNull.Value;

				void ThrowArgumentException() => throw new ArgumentException("\"" + key + "\" column is not found.");
			}//set
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		bool TryGetIndex(string key, out int index) => this.Schema.FieldMap.TryGetValue(key, out index);

		public IEnumerator<KeyValuePair<string, object>> GetEnumerator()
		{
			var fieldNames = this.Schema.FieldNames;
			var rowValues = this.RowValues;
			for (int i = 0; i < fieldNames.Length; ++i)
			{
				yield return new KeyValuePair<string, object>(fieldNames[i], RowValues[i]);
			}
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => this.GetEnumerator();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override string ToString() => string.Join(Environment.NewLine, this);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override int GetHashCode() => this.RowValues.GetHashCode();

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public override bool Equals(object obj) => this == (RowStore)obj;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator ==(RowStore rs1, RowStore rs2) => rs1.RowValues == rs2.RowValues;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static bool operator !=(RowStore rs1, RowStore rs2) => rs1.RowValues != rs2.RowValues;

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool ContainsKey(string key) => this.Schema.FieldMap.ContainsKey(key);

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public bool TryGetValue(string key, out object value)
		{
			if (!TryGetIndex(key, out var index))
			{
				value = notFound;
				return false;
			}
			value = this.RowValues[index];
			if (value == DBNull.Value)
				value = null;

			return true;
		}

		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public DynamicMetaObject GetMetaObject(Expression parameter) => new RowStoreMetaObject(parameter, BindingRestrictions.Empty, this);

		/// <summary>
		/// Converts RowStore into an instance of T on a best-effort-match basis. Does not throw on any mismatches.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public T ToObject<T>() where T : class, new() => ToObject<T>(New<T>.Instance);

		/// <summary>
		/// Converts RowStore into an instance of T on a best-effort-match basis. Does not throw on any mismatches.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public T ToObject<T>(Func<T> objectFactory) where T : class
		{
			var setters = ReflectionHelper_Setter<T>.Setters;
			var result = objectFactory();

			//var dbNullValue = DBNull.Value;
			var fieldNames = this.Schema.FieldNames;
			for (int i = 0; i < fieldNames.Length; ++i)
			{
				if (setters.TryGetValue(fieldNames[i], out var setter))
				{
					var val = this.RowValues[i];

					//if (val != dbNullValue)
					setter(result, val);
					//else setter(result, null);
				}
			}//for

			return result;
		}// ToObject<T>()

		/// <summary>
		/// Converts RowStore into an instance of T, and checks existence of all required (non-optional) properties. Failed checks throw an exception.
		/// </summary>
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public T ToCheckedObject<T>(string[] optionalProperties = null) where T : class, new() => ToCheckedObject<T>(New<T>.Instance, optionalProperties);

		/// <summary>
		/// Converts RowStore into an instance of T, and checks existence of all required (non-optional) properties. Failed checks throw an exception.
		/// </summary>
		public T ToCheckedObject<T>(Func<T> objectFactory, string[] optionalProperties = null) where T : class
		{
			var setters = ReflectionHelper_Setter<T>.Setters;
			var settersEnumerator = setters.GetEnumerator();
			var result = objectFactory();
			HashSet<string> optionalPropertyHashSet = null;

			while (settersEnumerator.MoveNext())
			{
				var kvp = settersEnumerator.Current;

				var val = this[kvp.Key];
				if (val != notFound)
					kvp.Value(result, val);
				else
				{
					if (optionalPropertyHashSet == null)
					{
						if (optionalProperties == null || optionalProperties.Length == 0) goto THROW;
						optionalPropertyHashSet = new HashSet<string>(optionalProperties, Util.FastStringComparer.Instance);
					}
					if (optionalPropertyHashSet.Contains(kvp.Key)) { continue; }
				THROW: throw new Exception(string.Format("RowStore has no match for class [{0}] property [{1}].", typeof(T), kvp.Key));
				}
			}//while
			return result;
		}// ToCheckedObject<T>()
	}// struct RowStore
	#endregion

	#region FieldNotFound
	public sealed class FieldNotFound
	{
		internal FieldNotFound() { }
		[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1065:DoNotRaiseExceptionsInUnexpectedLocations")]
		public sealed override string ToString() => throw new NotImplementedException(@"Field not found. Use ""obj is FieldNotFound"" instead of .ToString().");
	}//class FieldNotFound
	#endregion
}//ns