#ifndef __COMMON_H__
#define __COMMON_H__

#include <assert.h>
#include <stdio.h>
#include <iostream>
#include <list>
#include <stdint.h>
#include <assert.h>
#include <string>
#include <vector>
#include <map>
#include <sstream>
#include <stdlib.h>
#include <string.h>
#include <signal.h>
#include <sys/time.h>
#include <dlfcn.h>
#include <tr1/memory>


#define REDIS_OK                                                           0
#define REDIS_ERROR                                                        -1
#define REDIS_AGAIN                                                        -2
#define REDIS_NOSCRIPT                                                     -3

#define REDIS_MOVED                                                        2
#define REDIS_ASK                                                          3
#define REDIS_ASKING                                                       4
#define REDIS_ASKING_END                                                   5
#define REDIS_TRYAGAIN                                                     6

#define REDIS_CLUSTER_NODES                                                40

/******************************************
 *  *cluster nodes
 *      *
 *******************************************/

#define CLUSTER_NODES_ID                                                    0
#define CLUSTER_NODES_ADDR                                                  1
#define CLUSTER_NODES_ROLE                                                  2
#define CLUSTER_NODES_SID                                                   3
#define CLUSTER_NODES_CONNECTED                                             7
#define CLUSTER_NODES_LENGTH                                                8
#define CLUSTER_NODES_SLAVE_LEN                                             7


typedef struct {
    std::string cluster_name;
}redis_str_t;




#endif //__COMMON_H__
