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

// On aarch64-linux with glibc, floatn.h typedefs like `typedef float _Float32;`
// use GCC extension type keywords that Zig's translate-c cannot parse.
// Block the entire floatn.h/floatn-common.h headers via include guards, then
// manually define only the macros that downstream glibc headers need.
#if defined(__aarch64__) && defined(__linux__)
// Block the problematic headers entirely
#define _BITS_FLOATN_H
#define _BITS_FLOATN_COMMON_H
// Provide the macros that other glibc headers check
#define __HAVE_FLOAT128 0
#define __HAVE_DISTINCT_FLOAT128 0
#define __HAVE_FLOAT64X 0
#define __HAVE_FLOAT64X_LONG_DOUBLE 0
#define __HAVE_FLOAT32 0
#define __HAVE_FLOAT64 0
#define __HAVE_DISTINCT_FLOAT32 0
#define __HAVE_DISTINCT_FLOAT64 0
#define __HAVE_FLOAT16 0
#define __HAVE_DISTINCT_FLOAT16 0
#define __HAVE_FLOATN_NOT_TYPEDEF 0
// Tell glibc not to declare _FloatN math/stdlib functions
#define __GLIBC_USE(F) 0
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
