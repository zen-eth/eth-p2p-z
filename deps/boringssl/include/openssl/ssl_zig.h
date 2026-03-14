// Wrapper header for Zig's translate-c.
// Disables _Pragma macros that translate-c cannot handle, then includes ssl.h.

// Override the pragma macros BEFORE any BoringSSL headers are included.
// BoringSSL's base.h guards these with #ifndef, so pre-defining them works.
// However, base.h uses #if defined(__GNUC__) without #ifndef guards, so we
// need a different approach: undefine __GNUC__ and __clang__ to take the
// no-op branch of the pragma macro definition.
#ifdef __GNUC__
#define _SAVED_GNUC __GNUC__
#undef __GNUC__
#endif

#ifdef __clang__
#define _SAVED_CLANG __clang__
#undef __clang__
#endif

#include <openssl/ssl.h>

// Restore compiler identification macros
#ifdef _SAVED_GNUC
#define __GNUC__ _SAVED_GNUC
#undef _SAVED_GNUC
#endif

#ifdef _SAVED_CLANG
#define __clang__ _SAVED_CLANG
#undef _SAVED_CLANG
#endif
