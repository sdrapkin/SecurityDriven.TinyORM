using System;
using System.Collections.Generic;
using System.Linq;

namespace SecurityDriven.TinyORM.Helpers
{
	using Utils;

	public class Snapshot
	{
		readonly Dictionary<string, object> snapshot;

		Snapshot(Dictionary<string, object> objDictionary)
		{
			this.snapshot = objDictionary;
		}//ctor

		/// <summary>Creates a predicate returning "true" for changed property names (ie. matching property names that have different values), or Snapshot.NoDifference predicate.</summary>
		public Predicate<string> Diff<T>(T obj) where T : class
		{
			var objDictionary = GetObjDictionary(obj);
			object val; string key;

			var diffSet = new HashSet<string>(Util.FastStringComparer.Instance);
			foreach (var kvp in snapshot)
			{
				if (objDictionary.TryGetValue(key = kvp.Key, out val) && !object.Equals(val, kvp.Value))
					diffSet.Add(key);
			};
			return diffSet.Count > 0 ? name => diffSet.Contains(name) : NoDifference;
		}// Diff()

		static Dictionary<string, object> GetObjDictionary<T>(T obj) where T : class
		{
			Dictionary<string, object> objDictionary;
			var rowStore = obj as RowStore;
			if (rowStore != null)
				objDictionary = rowStore.ToDictionary(kvp => kvp.Key, kvp => kvp.Value, Util.FastStringComparer.Instance);
			else
				objDictionary = ObjectFactory.ObjectToDictionary(obj);

			return objDictionary;
		}// GetObjDictionary()

		static readonly Predicate<string> noDifference = str => false;

		/// <summary>Always-false predicate returned by Snapshot.Diff() when it finds no differences.</summary>
		public static Predicate<string> NoDifference => noDifference;

		public static Snapshot Create<T>(T obj) where T : class
		{
			var objDictionary = GetObjDictionary(obj);
			return new Snapshot(objDictionary);
		}// Create()
	}//class Snapshot
}//ns