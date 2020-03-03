using System;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace SecurityDriven.TinyORM.Helpers
{
	public static class SequentialGuid
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		public static Guid NewSequentialGuid() => GuidStruct.NewSequentialGuid();

		#region LongStruct
		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		struct LongStruct
		{
			[FieldOffset(0)]
			public long Value;

			[FieldOffset(0)]
			public byte B0;
			[FieldOffset(1)]
			public byte B1;
			[FieldOffset(2)]
			public byte B2;
			[FieldOffset(3)]
			public byte B3;
			[FieldOffset(4)]
			public byte B4;
			[FieldOffset(5)]
			public byte B5;
			[FieldOffset(6)]
			public byte B6;
			[FieldOffset(7)]
			public byte B7;
		}// struct LongStruct
		#endregion

		#region GuidStruct
		[StructLayout(LayoutKind.Explicit, Pack = 1)]
		struct GuidStruct
		{
			[FieldOffset(0)]
			public Guid Value;

			[FieldOffset(0)]
			public int IntValue0123;
			[FieldOffset(4)]
			public short ShortValue45;
			[FieldOffset(10)]
			public short ShortValueAB;

			[FieldOffset(0)]
			public byte B0;
			[FieldOffset(1)]
			public byte B1;
			[FieldOffset(2)]
			public byte B2;
			[FieldOffset(3)]
			public byte B3;
			[FieldOffset(4)]
			public byte B4;
			[FieldOffset(5)]
			public byte B5;

			[FieldOffset(6)] // biased for GUID_v4: always 4x
			public byte B6;
			[FieldOffset(7)]
			public byte B7;
			[FieldOffset(8)] // biased in GUID_v4: [8x, 9x, Ax, Bx]
			public byte B8;
			[FieldOffset(9)]
			public byte B9;

			[FieldOffset(10)]
			public byte BA;
			[FieldOffset(11)]
			public byte BB;

			[FieldOffset(12)]
			public byte BC;
			[FieldOffset(13)]
			public byte BD;
			[FieldOffset(14)]
			public byte BE;
			[FieldOffset(15)]
			public byte BF;

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			public static Guid NewSequentialGuid()
			{
				var guidStruct = new GuidStruct { Value = Guid.NewGuid() };
				var uuidStruct = new GuidStruct { Value = GetSequentialUuid() };

				return new Guid(
					a: guidStruct.IntValue0123,
					b: guidStruct.ShortValue45,

					c: guidStruct.ShortValueAB,

					d: uuidStruct.B1,
					e: uuidStruct.B0,

					f: uuidStruct.B7,
					g: uuidStruct.B6,
					h: uuidStruct.B5,
					i: uuidStruct.B4,
					j: uuidStruct.B3,
					k: uuidStruct.B2);
			}// NewSequentialGuid()

			[System.Diagnostics.CodeAnalysis.SuppressMessage("Microsoft.Design", "CA1060:MovePInvokesToNativeMethodsClass")]
			[DllImport("rpcrt4.dll", SetLastError = true), System.Security.SuppressUnmanagedCodeSecurity]
			static extern int UuidCreateSequential(out Guid guid);

			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			static Guid GetSequentialUuid()
			{
				const int RPC_S_OK = 0;
				if (UuidCreateSequential(out var g) == RPC_S_OK) return g;
				return ThrowException();

				static Guid ThrowException() => throw new Exception("UuidCreateSequential did not return RPC_S_OK.");
			}//GetSequentialUuid()
		}// struct GuidStruct
		#endregion

		// https://tools.ietf.org/html/rfc4122 UUID zero-tick starts on 1582-October-15 (Gregorian reform to the Christian calendar)
		static long UuidStartTicks = new DateTime(1582, 10, 15, 0, 0, 1, 1, DateTimeKind.Utc).Ticks;
		public static DateTime ExtractDateTimeUtc(this Guid g)
		{
			var guidStruct = new GuidStruct { Value = g };
			var longStruct = new LongStruct
			{
				B2 = guidStruct.BF,
				B3 = guidStruct.BE,
				B4 = guidStruct.BD,
				B5 = guidStruct.BC,
				B6 = guidStruct.BB,
				B7 = (byte)(guidStruct.BA & 0x0F)
			};
			return new DateTime(longStruct.Value + UuidStartTicks, DateTimeKind.Utc);
		}// ExtractDateTimeUtc()

#if Experimental
		/// <summary>
		/// Returns SMALLEST guid with an approximate utc timestamp. Useful for time-based database range searches.  
		/// </summary>
		public static Guid LowerGuidForDateTimeUtc(DateTime utc)
		{
			var longStruct = new LongStruct { Value = utc.Ticks - UuidStartTicks };
			var guidStruct = new GuidStruct
			{
				BA = (byte)(longStruct.B7 & 0x0F),
				BB = longStruct.B6,
				BC = longStruct.B5,
				BD = longStruct.B4,
				BE = longStruct.B3,
				BF = longStruct.B2
			};

			return guidStruct.Value;
		}// LowerGuidForDateTimeUtc()

		/// <summary>
		/// Returns LARGEST guid with an approximate utc timestamp. Useful for time-based database range searches.  
		/// </summary>
		public static Guid UpperGuidForDateTimeUtc(DateTime utc)
		{
			var longStruct = new LongStruct { Value = utc.Ticks - UuidStartTicks };
			var guidStruct = new GuidStruct
			{
				IntValue0123 = -1, // 0xFF_FF_FF_FF
				ShortValue45 = -1, // 0xFF_FF
				B6 = 0xFF,
				B7 = 0xFF,
				B8 = 0xFF,
				B9 = 0xFF,

				BA = (byte)(longStruct.B7 | 0xF0),
				BB = longStruct.B6,
				BC = longStruct.B5,
				BD = longStruct.B4,
				BE = longStruct.B3,
				BF = longStruct.B2
			};

			return guidStruct.Value;
		}// UpperGuidForDateTimeUtc()
#endif
	}//class SequentialGuid
}//ns