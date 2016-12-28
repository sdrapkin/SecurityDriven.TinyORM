using System;
using System.Runtime.CompilerServices;

namespace SecurityDriven.TinyORM
{
	using Utils;

	/// <summary>
	/// CallerIdentity class.
	/// </summary>
	public class CallerIdentity
	{
		/// <summary>
		/// Initializes a new instance of the <see cref="CallerIdentity"/> class.
		/// </summary>
		/// <param name="userId">The user id.</param>
		public CallerIdentity(Guid userId)
		{
			this.UserId = userId;
		}//ctor

		Guid userId;
		internal byte[] userIdAsBytes;
		/// <summary>
		/// Gets the user id.
		/// </summary>
		/// <value>The user id.</value>
		public Guid UserId
		{
			[MethodImpl(MethodImplOptions.AggressiveInlining)]
			get
			{
				return userId;
			}
			private set
			{
				userId = value;
				if (value == Guid.Empty)
					userIdAsBytes = Util.ZeroLengthArray<byte>.Value;
				else
					userIdAsBytes = value.ToByteArray();
			}
		}//UserId

		/// <summary>
		/// Gets the anonymous caller identity.
		/// </summary>
		/// <value>The anonymous caller identity.</value>
		public static CallerIdentity Anonymous => anonymousIdentity;

		static CallerIdentity anonymousIdentity = new CallerIdentity(Guid.Empty);
	}// class CallerIdentity

	static class CallerIdentityExtensions
	{
		[MethodImpl(MethodImplOptions.AggressiveInlining)]
		internal static byte[] GetBytes(this CallerIdentity ci)
		{
			return ci.userIdAsBytes;
		}
	}//class CallerIdentityExtensions
}//ns