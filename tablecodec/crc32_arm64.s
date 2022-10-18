#include "textflag.h"

TEXT ·crc32Uint64Hash(SB), NOSPLIT, $0
    MOVD crc+0(FP), R0
    MOVD data+8(FP), R1
    CRC32CX R1, R0
    MOVD   R0, ret+16(FP)

    RET
    