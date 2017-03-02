#ifndef __REDIS_SLOT_H__
#define __REDIS_SLOT_H__

#include "common.h"

#ifdef __cplusplus
extern "C" {
#endif

unsigned int key_hash_slot(char *key, int keylen);

uint16_t crc16(const char *buf, int len);


#ifdef __cplusplus
}
#endif


#endif //__REDIS_SLOT_H__

