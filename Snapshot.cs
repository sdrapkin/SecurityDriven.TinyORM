using System;
using System.Collections.Generic;

namespace SecurityDriven.TinyORM
{
	using Utils;

	public class Snapshot
	{
		Snapshot(Dictionary<string, object> propertyMap)
		{
			this.propertyMap = propertyMap;
		}//ctor

		readonly Dictionary<string, object> propertyMap;

		static Dictionary<string, object> GetObjectPropertiesAsDictionary<T>(T obj) where T : class
		{
			Dictionary<string, object> propertyMap;
			var rowStore = obj as RowStore;
			if (rowStore != null)
			{
				propertyMap = new Dictionary<string, object>(rowStore.RowValues.Length, Util.FastStringComparer.Instance);
				foreach (var kvp in rowStore.Schema.FieldMap)
				{
					propertyMap.Add(kvp.Key, rowStore.RowValues[kvp.Value]);
				}
			}
			else propertyMap = ObjectFactory.ObjectToDictionary(obj);

			return propertyMap;
		}//GetObjectPropertiesAsDictionary<T>()

		Predicate<string> Diff(Dictionary<string, object> propertyMap)
		{
			string key;

			var diffSet = new Dictionary<string, bool>(this.propertyMap.Count, Util.FastStringComparer.Instance); // HashSet<T> does not have a "capacity" ctor; Value is ignored
			foreach (var kvp in this.propertyMap)
			{
				if (propertyMap.TryGetValue(key = kvp.Key, out var val) && !object.Equals(val, kvp.Value))
					diffSet.Add(key, default(bool));
			}
			return diffSet.Count > 0 ? propertyName => diffSet.ContainsKey(propertyName) : NoDifference;
		}// Diff()

		/// <summary>Returns a predicate for changed property names (those that name-match to another "snapshot" but have different values), or a "Snapshot.NoDifference" predicate.</summary>
		public Predicate<string> Diff(Snapshot snapshot)
		{
			if (snapshot.propertyMap == this.propertyMap) return NoDifference;
			var result = this.Diff(snapshot.propertyMap);
			return result;
		}// Diff(snapshot)

		/// <summary>Returns a predicate for changed property names (those that name-match to "obj" but have different values), or a "Snapshot.NoDifference" predicate.</summary>
		public Predicate<string> Diff<T>(T obj) where T : class
		{
			var externalPropertyMap = GetObjectPropertiesAsDictionary<T>(obj);
			var result = this.Diff(externalPropertyMap);
			return result;
		}// Diff<T>()

		/// <summary>Creates a Snapshot of "obj", which can then be compared (diff'ed) against another object or another Snapshot.</summary>
		public static Snapshot Create<T>(T obj) where T : class
		{
			var externalPropertyMap = GetObjectPropertiesAsDictionary(obj);
			return new Snapshot(externalPropertyMap);
		}// Create<T>()

		static readonly Predicate<string> noDifference = str => false;

		/// <summary>Represents a predicate that Snapshot.Diff() returns when no differences are found.</summary>
		public static Predicate<string> NoDifference => noDifference;
	}// class Snapshot
}//ns