using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Tkl.Jumbo
{
    /// <summary>
    /// Contains extension methods for the <see cref="Type"/> class.
    /// </summary>
    public static class TypeExtensions
    {
        /// <summary>
        /// Finds a specific generic interface implemented by a type based on the generic type definition of the interface.
        /// </summary>
        /// <param name="type">The type whose interfaces to check.</param>
        /// <param name="interfaceType">The generic type definition of the interface.</param>
        /// <returns>The instantiated generic interface type.</returns>
        public static Type FindGenericInterfaceType(this Type type, Type interfaceType)
        {
            if( type == null )
                throw new ArgumentNullException("type");
            if( interfaceType == null )
                throw new ArgumentNullException("interfaceType");
            // This is necessary because while in .Net you can use type.GetInterface with a generic interface type,
            // in Mono that only works if you specify the type arguments which is precisely what we don't want.
            Type[] interfaces = type.GetInterfaces();
            foreach( Type i in interfaces )
            {
                if( i.IsGenericType && i.GetGenericTypeDefinition() == interfaceType )
                    return i;
            }
            throw new ArgumentException(string.Format("Type {0} does not implement interface {1}.", type, interfaceType));
        }
    }
}
