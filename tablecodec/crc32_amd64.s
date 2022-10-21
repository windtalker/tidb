#include "textflag.h"

TEXT ·crc32Uint64Hash(SB), NOSPLIT, $0
    MOVQ crc+0(FP), SI
    MOVQ data+8(FP), DI
    CRC32Q DI, SI
    MOVQ   SI, ret+16(FP)

    RET
    